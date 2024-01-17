// Copyright 2023 The Native Link Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::OsString;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_lock::RwLock;
use async_trait::async_trait;
use bytes::BytesMut;
use filetime::{set_file_atime, FileTime};
use futures::stream::{StreamExt, TryStreamExt};
use futures::{join, Future, TryFutureExt};
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::{fs, DigestInfo};
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::metrics_utils::{Collector, CollectorState, MetricsComponent, Registry};
use nativelink_util::store_trait::{Store, UploadSizeInfo};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::task::spawn_blocking;
use tokio::time::{sleep, timeout, Sleep};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, info, warn};

use crate::cas_utils::is_zero_digest;

// Default size to allocate memory of the buffer when reading files.
const DEFAULT_BUFF_SIZE: usize = 32 * 1024;

#[derive(Debug)]
pub struct SharedContext {
    // Used in testing to know how many active drop() spawns are running.
    // TODO(allada) It is probably a good idea to use a spin lock during
    // destruction of the store to ensure that all files are actually
    // deleted (similar to how it is done in tests).
    pub active_drop_spawns: AtomicU64,
    temp_path: String,
    content_path: String,
}

#[derive(Eq, PartialEq, Debug)]
enum PathType {
    Content,
    Temp,
}

// Note: We don't store the full path of the file because it would cause
// a lot of needless memeory bloat. There's a high chance we'll end up with a
// lot of small files, so to prevent storing duplicate data, we store an Arc
// to the path of the directory where the file is stored and the packed digest.
// Resulting in usize + sizeof(DigestInfo).
type FileNameDigest = DigestInfo;
pub struct EncodedFilePath {
    shared_context: Arc<SharedContext>,
    path_type: PathType,
    digest: FileNameDigest,
}

impl EncodedFilePath {
    #[inline]
    fn get_file_path(&self) -> OsString {
        get_file_path_raw(&self.path_type, self.shared_context.as_ref(), &self.digest)
    }
}

#[inline]
fn get_file_path_raw(path_type: &PathType, shared_context: &SharedContext, digest: &DigestInfo) -> OsString {
    let folder = match path_type {
        PathType::Content => &shared_context.content_path,
        PathType::Temp => &shared_context.temp_path,
    };
    to_full_path_from_digest(folder, digest)
}

impl Drop for EncodedFilePath {
    fn drop(&mut self) {
        // `drop()` can be called during shutdown, so we use `path_type` flag to know if the
        // file actually needs to be deleted.
        if self.path_type == PathType::Content {
            return;
        }

        let file_path = self.get_file_path();
        let shared_context = self.shared_context.clone();
        shared_context.active_drop_spawns.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            info!("\x1b[0;31mFilesystem Store\x1b[0m: Deleting: {:?}", file_path);
            let result = fs::remove_file(&file_path)
                .await
                .err_tip(|| format!("Failed to remove file {:?}", file_path));
            if let Err(err) = result {
                error!("\x1b[0;31mFilesystem Store\x1b[0m: {:?}", err);
            }
            shared_context.active_drop_spawns.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

#[inline]
fn to_full_path_from_digest(folder: &str, digest: &DigestInfo) -> OsString {
    format!("{}/{}-{}", folder, digest.hash_str(), digest.size_bytes).into()
}

#[async_trait]
pub trait FileEntry: LenEntry + Send + Sync + Debug + 'static {
    /// Responsible for creating the underlying FileEntry.
    fn create(file_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self;

    /// Creates a (usually) temp file, opens it and returns the path to the temp file.
    async fn make_and_open_file(
        encoded_file_path: EncodedFilePath,
    ) -> Result<(Self, fs::ResumeableFileSlot<'static>, OsString), Error>
    where
        Self: Sized;

    /// Returns the underlying reference to where the filesize is stored.
    fn get_file_size(&mut self) -> &mut u64;

    /// Gets the underlying EncodedfilePath.
    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath>;

    /// Returns a reader that will read part of the underlying file.
    async fn read_file_part<'a>(&'a self, offset: u64, length: u64) -> Result<fs::ResumeableFileSlot<'a>, Error>;

    /// This function is a safe way to extract the file name of the underlying file. To protect users from
    /// accidentally creating undefined behavior we encourage users to do the logic they need to do with
    /// the filename inside this function instead of extracting the filename and doing the logic outside.
    /// This is because the filename is not guaranteed to exist after this function returns, however inside
    /// the callback the file is always guaranteed to exist and immutable.
    /// DO NOT USE THIS FUNCTION TO EXTRACT THE FILENAME AND STORE IT FOR LATER USE.
    async fn get_file_path_locked<T, Fut: Future<Output = Result<T, Error>> + Send, F: FnOnce(OsString) -> Fut + Send>(
        &self,
        handler: F,
    ) -> Result<T, Error>;
}

pub struct FileEntryImpl {
    file_size: u64,
    encoded_file_path: RwLock<EncodedFilePath>,
}

impl FileEntryImpl {
    pub fn get_shared_context_for_test(&mut self) -> Arc<SharedContext> {
        self.encoded_file_path.get_mut().shared_context.clone()
    }
}

#[async_trait]
impl FileEntry for FileEntryImpl {
    fn create(file_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self {
        Self {
            file_size,
            encoded_file_path,
        }
    }

    /// This encapsolates the logic for the edge case of if the file fails to create
    /// the cleanup of the file is handled without creating a FileEntry, which would
    /// try to cleanup the file as well during drop().
    async fn make_and_open_file(
        encoded_file_path: EncodedFilePath,
    ) -> Result<(FileEntryImpl, fs::ResumeableFileSlot<'static>, OsString), Error> {
        let temp_full_path = encoded_file_path.get_file_path();
        let temp_file_result = fs::create_file(temp_full_path.clone())
            .or_else(|mut err| async {
                let remove_result = fs::remove_file(&temp_full_path)
                    .await
                    .err_tip(|| format!("Failed to remove file {:?} in filesystem store", temp_full_path));
                if let Err(remove_err) = remove_result {
                    err = err.merge(remove_err);
                }
                warn!("\x1b[0;31mFilesystem Store\x1b[0m: {:?}", err);
                Err(err).err_tip(|| format!("Failed to create {:?} in filesystem store", temp_full_path))
            })
            .await?;

        Ok((
            <FileEntryImpl as FileEntry>::create(
                0, /* Unknown yet, we will fill it in later */
                RwLock::new(encoded_file_path),
            ),
            temp_file_result,
            temp_full_path,
        ))
    }

    fn get_file_size(&mut self) -> &mut u64 {
        &mut self.file_size
    }

    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath> {
        &self.encoded_file_path
    }

    async fn read_file_part<'a>(&'a self, offset: u64, length: u64) -> Result<fs::ResumeableFileSlot<'a>, Error> {
        let (mut file, full_content_path_for_debug_only) = self
            .get_file_path_locked(|full_content_path| async move {
                let file = fs::open_file(full_content_path.clone(), length)
                    .await
                    .err_tip(|| format!("Failed to open file in filesystem store {:?}", full_content_path))?;
                Ok((file, full_content_path))
            })
            .await?;

        file.as_reader()
            .await
            .err_tip(|| "Could not seek file in read_file_part()")?
            .get_mut()
            .seek(SeekFrom::Start(offset))
            .await
            .err_tip(|| format!("Failed to seek file: {:?}", full_content_path_for_debug_only))?;
        Ok(file)
    }

    async fn get_file_path_locked<
        T,
        Fut: Future<Output = Result<T, Error>> + Send,
        F: FnOnce(OsString) -> Fut + Send,
    >(
        &self,
        handler: F,
    ) -> Result<T, Error> {
        let encoded_file_path = self.get_encoded_file_path().read().await;
        handler(encoded_file_path.get_file_path()).await
    }
}

impl Debug for FileEntryImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("FileEntryImpl")
            .field("file_size", &self.file_size)
            .field("encoded_file_path", &"<behind mutex>")
            .finish()
    }
}

fn make_temp_digest(digest: &mut DigestInfo) {
    static DELETE_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
    digest.packed_hash[24..].clone_from_slice(&DELETE_FILE_COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
}

#[async_trait]
impl LenEntry for FileEntryImpl {
    #[inline]
    fn len(&self) -> usize {
        self.file_size as usize
    }

    fn is_empty(&self) -> bool {
        self.file_size == 0
    }

    #[inline]
    async fn touch(&self) {
        let result = self
            .get_file_path_locked(move |full_content_path| async move {
                spawn_blocking(move || {
                    set_file_atime(&full_content_path, FileTime::now())
                        .err_tip(|| format!("Failed to touch file in filesystem store {:?}", full_content_path))
                })
                .await
                .map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to change atime of file due to spawn failing {:?}",
                        e
                    )
                })?
            })
            .await;
        if let Err(e) = result {
            error!("{}", e);
        }
    }

    // unref() only triggers when an item is removed from the eviction_map. It is possible
    // that another place in code has a reference to `FileEntryImpl` and may later read the
    // file. To support this edge case, we first move the file to a temp file and point
    // target file location to the new temp file. `unref()` should only ever be called once.
    #[inline]
    async fn unref(&self) {
        {
            let mut encoded_file_path = self.encoded_file_path.write().await;
            if encoded_file_path.path_type == PathType::Temp {
                // We are already a temp file that is now marked for deletion on drop.
                // This is very rare, but most likely the rename into the content path failed.
                return;
            }
            let from_path = encoded_file_path.get_file_path();
            let mut new_digest = encoded_file_path.digest;
            make_temp_digest(&mut new_digest);

            let to_path = to_full_path_from_digest(&encoded_file_path.shared_context.temp_path, &new_digest);

            info!(
                "\x1b[0;31mFilesystem Store\x1b[0m: Unref {}, moving file {:?} to {:?}",
                encoded_file_path.digest.hash_str(),
                from_path,
                to_path
            );
            if let Err(err) = fs::rename(&from_path, &to_path).await {
                warn!(
                    "Failed to rename file from {:?} to {:?} : {:?}",
                    from_path, to_path, err
                );
            } else {
                encoded_file_path.path_type = PathType::Temp;
                encoded_file_path.digest = new_digest;
            }
        }
    }
}

#[inline]
pub fn digest_from_filename(file_name: &str) -> Result<DigestInfo, Error> {
    let (hash, size) = file_name.split_once('-').err_tip(|| "")?;
    let size = size.parse::<i64>()?;
    DigestInfo::try_new(hash, size)
}

/// The number of files to read the metadata for at the same time when running
/// add_files_to_cache.
const SIMULTANEOUS_METADATA_READS: usize = 200;

async fn add_files_to_cache<Fe: FileEntry>(
    evicting_map: &EvictingMap<Arc<Fe>, SystemTime>,
    anchor_time: &SystemTime,
    shared_context: &Arc<SharedContext>,
) -> Result<(), Error> {
    async fn process_entry<Fe: FileEntry>(
        evicting_map: &EvictingMap<Arc<Fe>, SystemTime>,
        file_name: &str,
        atime: SystemTime,
        file_size: u64,
        anchor_time: &SystemTime,
        shared_context: &Arc<SharedContext>,
    ) -> Result<(), Error> {
        let digest = digest_from_filename(file_name)?;

        let file_entry = Fe::create(
            file_size,
            RwLock::new(EncodedFilePath {
                shared_context: shared_context.clone(),
                path_type: PathType::Content,
                digest,
            }),
        );
        let time_since_anchor = anchor_time
            .duration_since(atime)
            .map_err(|_| make_input_err!("File access time newer than now"))?;
        evicting_map
            .insert_with_time(digest, Arc::new(file_entry), time_since_anchor.as_secs() as i32)
            .await;
        Ok(())
    }

    let mut file_infos: Vec<(String, SystemTime, u64)> = {
        let (_permit, dir_handle) = fs::read_dir(format!("{}/", shared_context.content_path))
            .await
            .err_tip(|| "Failed opening content directory for iterating in filesystem store")?
            .into_inner();

        let read_dir_stream = ReadDirStream::new(dir_handle);
        read_dir_stream
            .map(|dir_entry| async move {
                let dir_entry = dir_entry.unwrap();
                let file_name = dir_entry.file_name().into_string().unwrap();
                let metadata = dir_entry
                    .metadata()
                    .await
                    .err_tip(|| "Failed to get metadata in filesystem store")?;
                let atime = match metadata.accessed() {
                    Ok(atime) => atime,
                    Err(err) => {
                        panic!(
                            "{}{}{} : {} {:?}",
                            "It appears this filesystem does not support access time. ",
                            "Please configure this program to run on a drive that supports ",
                            "atime",
                            file_name,
                            err
                        );
                    }
                };
                Result::<(String, SystemTime, u64), Error>::Ok((file_name, atime, metadata.len()))
            })
            .buffer_unordered(SIMULTANEOUS_METADATA_READS)
            .try_collect()
            .await?
    };

    file_infos.sort_by(|a, b| a.1.cmp(&b.1));
    for (file_name, atime, file_size) in file_infos {
        let result = process_entry(evicting_map, &file_name, atime, file_size, anchor_time, shared_context).await;
        if let Err(err) = result {
            warn!(
                "Could not add file to eviction cache, so deleting: {} - {:?}",
                file_name, err
            );
            // Ignore result.
            let _ = fs::remove_file(format!("{}/{}", &shared_context.content_path, &file_name)).await;
        }
    }
    Ok(())
}

async fn prune_temp_path(temp_path: &str) -> Result<(), Error> {
    let (_permit, dir_handle) = fs::read_dir(temp_path)
        .await
        .err_tip(|| "Failed opening temp directory to prune partial downloads in filesystem store")?
        .into_inner();

    let mut read_dir_stream = ReadDirStream::new(dir_handle);
    while let Some(dir_entry) = read_dir_stream.next().await {
        let path = dir_entry?.path();
        if let Err(err) = fs::remove_file(&path).await {
            warn!("Failed to delete file in filesystem store {:?} : {:?}", &path, err);
        }
    }
    Ok(())
}

pub struct FilesystemStore<Fe: FileEntry = FileEntryImpl> {
    shared_context: Arc<SharedContext>,
    evicting_map: Arc<EvictingMap<Arc<Fe>, SystemTime>>,
    read_buffer_size: usize,
    sleep_fn: fn(Duration) -> Sleep,
    rename_fn: fn(&OsString, &OsString) -> Result<(), std::io::Error>,
}

impl<Fe: FileEntry> FilesystemStore<Fe> {
    pub async fn new(config: &nativelink_config::stores::FilesystemStore) -> Result<Self, Error> {
        Self::new_with_timeout_and_rename_fn(config, sleep, |from, to| std::fs::rename(from, to)).await
    }

    pub async fn new_with_timeout_and_rename_fn(
        config: &nativelink_config::stores::FilesystemStore,
        sleep_fn: fn(Duration) -> Sleep,
        rename_fn: fn(&OsString, &OsString) -> Result<(), std::io::Error>,
    ) -> Result<Self, Error> {
        let now = SystemTime::now();

        let empty_policy = nativelink_config::stores::EvictionPolicy::default();
        let eviction_policy = config.eviction_policy.as_ref().unwrap_or(&empty_policy);
        let evicting_map = Arc::new(EvictingMap::new(eviction_policy, now));

        fs::create_dir_all(&config.temp_path)
            .await
            .err_tip(|| format!("Failed to temp directory {:?}", &config.temp_path))?;
        fs::create_dir_all(&config.content_path)
            .await
            .err_tip(|| format!("Failed to content directory {:?}", &config.content_path))?;

        let shared_context = Arc::new(SharedContext {
            active_drop_spawns: AtomicU64::new(0),
            temp_path: config.temp_path.clone(),
            content_path: config.content_path.clone(),
        });
        add_files_to_cache(evicting_map.as_ref(), &now, &shared_context).await?;
        prune_temp_path(&shared_context.temp_path).await?;

        let read_buffer_size = if config.read_buffer_size == 0 {
            DEFAULT_BUFF_SIZE
        } else {
            config.read_buffer_size as usize
        };
        let store = Self {
            shared_context,
            evicting_map,
            read_buffer_size,
            sleep_fn,
            rename_fn,
        };
        Ok(store)
    }

    pub async fn get_file_entry_for_digest(&self, digest: &DigestInfo) -> Result<Arc<Fe>, Error> {
        self.evicting_map
            .get(digest)
            .await
            .ok_or_else(|| make_err!(Code::NotFound, "{} not found in filesystem store", digest.hash_str()))
    }

    async fn update_file<'a>(
        self: Pin<&'a Self>,
        mut entry: Fe,
        mut resumeable_temp_file: fs::ResumeableFileSlot<'a>,
        final_digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<(), Error> {
        let mut file_size = 0;
        loop {
            let Ok(data_result) = timeout(fs::idle_file_descriptor_timeout(), reader.recv()).await else {
                // In the event we timeout, we want to close the writing file, to prevent
                // the file descriptor left open for long periods of time.
                // This is needed because we wrap `fs` so only a fixed number of file
                // descriptors may be open at any given time. If we are streaming from
                // File -> File, it can cause a deadlock if the Write file is not sending
                // data because it is waiting for a file descriotor to open before sending data.
                resumeable_temp_file
                    .close_file()
                    .await
                    .err_tip(|| "Could not close file due to timeout in FileSystemStore::update_file")?;
                continue;
            };
            let mut data = data_result.err_tip(|| "Failed to receive data in filesystem store")?;
            let data_len = data.len();
            if data_len == 0 {
                break; // EOF.
            }
            resumeable_temp_file
                .as_writer()
                .await
                .err_tip(|| "in filesystem_store::update_file")?
                .write_all_buf(&mut data)
                .await
                .err_tip(|| "Failed to write data into filesystem store")?;
            file_size += data_len as u64;
        }

        resumeable_temp_file
            .as_writer()
            .await
            .err_tip(|| "in filesystem_store::update_file")?
            .as_ref()
            .sync_all()
            .await
            .err_tip(|| "Failed to sync_data in filesystem store")?;

        drop(resumeable_temp_file);

        *entry.get_file_size() = file_size;
        let entry = Arc::new(entry);

        // This sequence of events is quite ticky to understand due to the amount of triggers that
        // happen, async'ness of it and the locking. So here is a breakdown of what happens:
        // 1. Here will hold a write lock on any file operations of this FileEntry.
        // 2. Then insert the entry into the evicting map. This may trigger an eviction of other
        //    entries.
        // 3. Eviction triggers `unref()`, which grabs a write lock on the evicted FileEntrys
        //    during the rename.
        // 4. It should be impossible for items to be added while eviction is happening, so there
        //    should not be a deadlock possability. However, it is possible for the new FileEntry
        //    to be evicted before the file is moved into place. Eviction of the newly inserted
        //    item is not possible within the `insert()` call because the write lock inside the
        //    eviction map. If an eviction of new item happens after `insert()` but before
        //    `rename()` then we get to finish our operation because the `unref()` of the new item
        //    will be blocked on us because we currently have the lock.
        // 5. Move the file into place. Since we hold a write lock still anyone that gets our new
        //    FileEntry (which has not yet been placed on disk) will not be able to read the file's
        //    contents until we relese the lock.
        let evicting_map = self.evicting_map.clone();
        let rename_fn = self.rename_fn;

        // We need to guarantee that this will get to the end even if the parent future is dropped.
        // See: https://github.com/TraceMachina/nativelink/issues/495
        tokio::spawn(async move {
            let mut encoded_file_path = entry.get_encoded_file_path().write().await;
            let final_path = get_file_path_raw(
                &PathType::Content,
                encoded_file_path.shared_context.as_ref(),
                &final_digest,
            );

            evicting_map.insert(final_digest, entry.clone()).await;

            let from_path = encoded_file_path.get_file_path();
            // Internally tokio spawns fs commands onto a blocking thread anyways.
            // Since we are already on a blocking thread, we just need the `fs` wrapper to manage
            // an open-file permit (ensure we don't open too many files at once).
            let result = fs::call_with_permit(|| {
                (rename_fn)(&from_path, &final_path)
                    .err_tip(|| format!("Failed to rename temp file to final path {final_path:?}"))
            }).await;

            // In the event our move from temp file to final file fails we need to ensure we remove
            // the entry from our map.
            // Remember: At this point it is possible for another thread to have a reference to
            // `entry`, so we can't delete the file, only drop() should ever delete files.
            if let Err(err) = result {
                warn!("Error while renaming file: {err} - {from_path:?} -> {final_path:?}");
                // Warning: To prevent deadlock we need to release our lock or during `remove_if()`
                // it will call `unref()`, which triggers a write-lock on `encoded_file_path`.
                drop(encoded_file_path);
                // It is possible that the item in our map is no longer the item we inserted,
                // So, we need to conditionally remove it only if the pointers are the same.
                evicting_map
                    .remove_if(&final_digest, |map_entry| Arc::<Fe>::ptr_eq(map_entry, &entry))
                    .await;
                return Err(err);
            }
            encoded_file_path.path_type = PathType::Content;
            encoded_file_path.digest = final_digest;
            Ok(())
        })
        .await
        .err_tip(|| "Failed to create spawn in filesystem store update_file")?
    }
}

#[async_trait]
impl<Fe: FileEntry> Store for FilesystemStore<Fe> {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[DigestInfo],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        self.evicting_map.sizes_for_keys(digests, results).await;
        // We need to do a special pass to ensure our zero files exist.
        // If our results failed and the result was a zero file, we need to
        // create the file by spec.
        for (digest, result) in digests.iter().zip(results.iter_mut()) {
            if result.is_some() || !is_zero_digest(digest) {
                continue;
            }
            let (mut tx, rx) = make_buf_channel_pair();
            let update_fut = self.update(*digest, rx, UploadSizeInfo::ExactSize(0));
            let (update_result, send_eof_result) = join!(update_fut, tx.send_eof());
            update_result
                .err_tip(|| format!("Failed to create zero file for digest {digest:?}"))
                .merge(send_eof_result.err_tip(|| "Failed to send zero file EOF in filesystem store has"))?;

            *result = Some(0);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let mut temp_digest = digest;
        make_temp_digest(&mut temp_digest);

        let (entry, temp_file, temp_full_path) = Fe::make_and_open_file(EncodedFilePath {
            shared_context: self.shared_context.clone(),
            path_type: PathType::Temp,
            digest: temp_digest,
        })
        .await?;

        self.update_file(entry, temp_file, digest, reader)
            .await
            .err_tip(|| format!("While processing with temp file {:?}", temp_full_path))
    }

    async fn get_part_ref(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        if is_zero_digest(&digest) {
            self.has(digest)
                .await
                .err_tip(|| "Failed to check if zero digest exists in filesystem store")?;
            writer
                .send_eof()
                .await
                .err_tip(|| "Failed to send zero EOF in filesystem store get_part_ref")?;
            return Ok(());
        }

        let entry = self
            .evicting_map
            .get(&digest)
            .await
            .ok_or_else(|| make_err!(Code::NotFound, "{} not found in filesystem store", digest.hash_str()))?;
        let read_limit = length.unwrap_or(usize::MAX) as u64;
        let mut resumeable_temp_file = entry.read_file_part(offset as u64, read_limit).await?;

        let mut buf = BytesMut::with_capacity(length.unwrap_or(self.read_buffer_size));
        loop {
            resumeable_temp_file
                .as_reader()
                .await
                .err_tip(|| "In FileSystemStore::get_part()")?
                .read_buf(&mut buf)
                .await
                .err_tip(|| "Failed to read data in filesystem store")?;
            if buf.is_empty() {
                break; // EOF.
            }
            // In the event it takes a while to send the data to the client, we want to close the
            // reading file, to prevent the file descriptor left open for long periods of time.
            // Failing to do so might cause deadlocks if the receiver is unable to receive data
            // because it is waiting for a file descriptor to open before receiving data.
            // Using `ResumeableFileSlot` will re-open the file in the event it gets closed on the
            // next iteration.
            let buf_content = buf.split().freeze();
            loop {
                let sleep_fn = (self.sleep_fn)(fs::idle_file_descriptor_timeout());
                tokio::pin!(sleep_fn);
                tokio::select! {
                    _ = & mut (sleep_fn) => {
                        resumeable_temp_file
                            .close_file()
                            .await
                            .err_tip(|| "Could not close file due to timeout in FileSystemStore::get_part")?;
                        continue;
                    }
                    res = writer.send(buf_content.clone()) => {
                        match res {
                            Ok(()) => break,
                            Err(err) => {
                                return Err(err).err_tip(|| "Failed to send chunk in filesystem store get_part");
                            }
                        }
                    }
                }
            }
        }
        writer
            .send_eof()
            .await
            .err_tip(|| "Filed to send EOF in filesystem store get_part")?;

        Ok(())
    }

    fn inner_store(self: Arc<Self>, _digest: Option<DigestInfo>) -> Arc<dyn Store> {
        self
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(self)
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }
}

impl<Fe: FileEntry> MetricsComponent for FilesystemStore<Fe> {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "read_buff_size_bytes",
            &self.read_buffer_size,
            "Size of the configured read buffer size",
        );
        c.publish(
            "active_drop_spawns_total",
            &self.shared_context.active_drop_spawns,
            "Number of active drop spawns",
        );
        c.publish(
            "temp_path",
            &self.shared_context.temp_path,
            "Path to the configured temp path",
        );
        c.publish(
            "content_path",
            &self.shared_context.content_path,
            "Path to the configured content path",
        );
        c.publish("evicting_map", self.evicting_map.as_ref(), "");
    }
}
