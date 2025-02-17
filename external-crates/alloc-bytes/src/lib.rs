// Copyright 2025 The NativeLink Authors. All rights reserved.
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

#![no_std]
//! # `alloc_bytes`
//!
//! `alloc_bytes` is a crate that provides custom byte buffers allocated using a user-provided allocator.
//! It offers both mutable and immutable byte buffers, enabling efficient memory allocation strategies
//! in systems with custom memory requirements.
//!
//! ## Features
//!
//! - **Custom Allocator Support:** Allocate bytes via any allocator implementing [`GlobalAlloc`].
//! - **Mutable & Immutable Buffers:** Build data with [`AllocBytesMut`] and freeze it into an immutable
//!   [`AllocBytes`].
//! - **Interoperability with the `bytes` Crate:** Convert to [`bytes::Bytes`] when the `bytes`
//!   feature is enabled.
//!
//! ## Usage Example
//!
//! ```rust
//! use alloc_bytes::AllocBytesMut;
//! use heap_allocator::HeapAllocator;
//! use bytes::Bytes;
//!
//! const SLAB_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
//! let raw_slab = Vec::with_capacity(SLAB_SIZE);
//! let allocator = HeapAllocator::new(raw_slab);
//!
//! let mut buffer = AllocBytesMut::new_unaligned(&allocator);
//! buffer.extend_from_slice(b"Hello, world!").unwrap();
//! let bytes_data: Bytes = buffer.into_bytes();
//!
//! assert_eq!(bytes_data.as_ref(), b"Hello, world!");
//! ```
//!
//! ## Related Crates and Resources
//!
//! - **[bytes](https://docs.rs/bytes):** Utilities for working with byte buffers.
//! - **[heap_allocator](https://crates.io/crates/heap_allocator):** An example custom heap allocator.
//!
//! This crate is `no_std` compatible.
use core::alloc::{GlobalAlloc, Layout};
use core::ptr::NonNull;

#[cfg(feature = "bytes")]
use bytes::Bytes;

/// Errors that can occur during allocation or reallocation.
#[derive(Debug)]
pub enum AllocBytesError {
    /// Returned when allocation or reallocation fails.
    FailedToRealloc,
    /// Returned when the requested capacity addition would overflow.
    ReserveTooLarge,
    /// Returned when the requested layout is invalid.
    Layout(core::alloc::LayoutError),
}

/// An immutable byte buffer allocated via a custom allocator.
///
/// This type holds a raw pointer to memory allocated using the provided allocator.
/// It is not [`Sync`] because the inner pointer is mutated in controlled ways.
/// If you need to share the buffer across threads concurrently (i.e. require [`Sync`]),
/// consider converting it into a [`Bytes`] object via [`AllocBytesMut::into_bytes()`]
/// or wrapping it in an [`std::sync::Arc`].
///
/// # Type Parameters
/// - `T`: A type that can be converted to a reference to the allocator.
/// - `A`: The allocator type that implements [`GlobalAlloc`].
/// - `ALIGN`: The alignment to use for allocations (default is 1).
pub struct AllocBytes<T: AsRef<A>, A: GlobalAlloc + ?Sized, const ALIGN: usize = 1> {
    heap: T,
    ptr: NonNull<u8>,
    size: usize,
    cap: usize,
    _phantom: core::marker::PhantomData<A>,
}

/// Safety: Although [`AllocBytes`] contains a raw pointer, it is only mutated during
/// deallocation (in [`Drop`]) and via exclusive mutable access in [`AllocBytesMut`].
/// Thus, it is safe to implement [`Send`] if the underlying allocator is thread-safe.
unsafe impl<T: AsRef<A>, A: GlobalAlloc + ?Sized, const ALIGN: usize> Send
    for AllocBytes<T, A, ALIGN>
{
}

impl<T: AsRef<A>, A: GlobalAlloc, const ALIGN: usize> AsRef<[u8]> for AllocBytes<T, A, ALIGN> {
    /// Returns a byte slice of the allocated memory.
    ///
    /// # Safety
    ///
    /// This is safe because the invariants of [`AllocBytes`] ensure that the pointer is valid
    /// for [`AllocBytes::size`] bytes.
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }
}

impl<T: AsRef<A>, A: GlobalAlloc + ?Sized, const ALIGN: usize> Drop for AllocBytes<T, A, ALIGN> {
    /// Deallocates the allocated memory.
    ///
    /// # Safety
    ///
    /// The memory was allocated with a layout of size [`AllocBytes::cap`] and alignment `ALIGN`.
    /// If `ptr` is still [`NonNull::dangling()`], no deallocation is performed.
    fn drop(&mut self) {
        if self.ptr == NonNull::dangling() {
            // No allocation was ever performed.
            return;
        }
        unsafe {
            self.heap.as_ref().dealloc(
                self.ptr.as_ptr(),
                // SAFETY: The layout here matches the one used during [re]allocation.
                Layout::from_size_align_unchecked(self.cap, ALIGN),
            );
        }
    }
}

/// A mutable byte buffer for building up data with a custom allocator.
///
/// This type allows extending the buffer and then "freezing" it into an immutable
/// [`AllocBytes`] (using [`AllocBytesMut::freeze()`] and/or [`Bytes`] (using
/// [`AllocBytesMut::into_bytes()`]).
pub struct AllocBytesMut<T: AsRef<A>, A: GlobalAlloc + ?Sized, const ALIGN: usize>(
    AllocBytes<T, A, ALIGN>,
);

impl<T: AsRef<A>, A: GlobalAlloc> AllocBytesMut<T, A, 1> {
    /// Creates a new mutable byte buffer with unaligned allocations
    /// (ie: alignment = 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use alloc_bytes::AllocBytesMut;
    /// use bytes::Bytes;
    /// use heap_allocator::HeapAllocator;
    ///
    /// // 64 MiB slab size for allocator.
    /// const SLAB_SIZE: usize = 64 * 1024 * 1024;
    /// let raw_slab = Vec::with_capacity(SLAB_SIZE);
    /// let allocator = HeapAllocator::new(raw_slab);
    /// let mut buffer = AllocBytesMut::new_unaligned(&allocator);
    ///
    /// buffer.extend_from_slice(b"Hello, world!");
    /// assert_eq!(buffer.len(), 13);
    /// let bytes_data: Bytes = buffer.into_bytes();
    /// assert_eq!(bytes_data.as_ref(), b"Hello, world!");
    ///
    /// // [`HeapAllocator::dealloc`](bytes_data.ptr, ...) is called here.
    /// drop(bytes_data);
    /// ```
    pub fn new_unaligned(heap: T) -> Self {
        Self(AllocBytes {
            heap,
            // Use dangling pointer as a sentinel for "no allocation".
            ptr: NonNull::dangling(),
            size: 0,
            cap: 0,
            _phantom: core::marker::PhantomData,
        })
    }
}

impl<T: AsRef<A>, A: GlobalAlloc, const ALIGN: usize> AllocBytesMut<T, A, ALIGN> {
    /// Creates a new mutable byte buffer.
    ///
    /// This is the same as [`AllocBytesMut::new_unaligned()`] but used
    /// when `ALIGN` needs to be > 1.
    ///
    /// See: [`AllocBytesMut::new_unaligned()`]
    pub fn new(heap: T) -> Self {
        Self(AllocBytes {
            heap,
            ptr: NonNull::dangling(),
            size: 0,
            cap: 0,
            _phantom: core::marker::PhantomData,
        })
    }

    /// Returns the current capacity of the buffer in bytes.
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.0.cap
    }

    /// Returns the current length (number of bytes used) of the buffer.
    #[inline]
    pub const fn len(&self) -> usize {
        self.0.size
    }

    /// Returns `true` if the buffer is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ensures that the buffer has at least `additional` extra bytes of capacity.
    ///
    /// If the current capacity is insufficient, the buffer is reallocated.
    ///
    /// # Errors
    ///
    /// Returns an error if the new capacity calculation overflows, the layout is invalid,
    /// or the allocation/reallocation fails.
    ///
    /// Data, length, and capacity are left unchanged if an error occurs.
    #[inline]
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocBytesError> {
        let len = self.len();
        debug_assert!(
            self.0.cap >= len,
            "Capacity is less than length in AllocBytesMut::reserve"
        );
        // SAFETY: `AllocBytes::cap` is always >= `AllocBytes::size`.
        let remaining = unsafe { self.0.cap.unchecked_sub(len) };
        if additional <= remaining {
            return Ok(());
        }
        let new_capacity = len
            .checked_add(additional)
            .ok_or(AllocBytesError::ReserveTooLarge)?;
        let new_ptr = unsafe {
            if self.0.ptr == NonNull::dangling() {
                let layout = if ALIGN == 1 {
                    // SAFETY: For alignment 1, layout creation cannot fail.
                    Layout::from_size_align_unchecked(new_capacity, ALIGN)
                } else {
                    Layout::from_size_align(new_capacity, ALIGN).map_err(AllocBytesError::Layout)?
                };
                self.0.heap.as_ref().alloc(layout)
            } else {
                let layout = if ALIGN == 1 {
                    // SAFETY: For alignment 1, layout creation cannot fail.
                    Layout::from_size_align_unchecked(self.0.cap, ALIGN)
                } else {
                    Layout::from_size_align(self.0.cap, ALIGN).map_err(AllocBytesError::Layout)?
                };
                self.0
                    .heap
                    .as_ref()
                    .realloc(self.0.ptr.as_ptr(), layout, new_capacity)
            }
        };
        let new_ptr = NonNull::new(new_ptr).ok_or(AllocBytesError::FailedToRealloc)?;
        self.0.cap = new_capacity;
        self.0.ptr = new_ptr;
        Ok(())
    }

    /// Extends the buffer by copying bytes from the provided slice.
    ///
    /// # Errors
    ///
    /// Returns an error if reserving additional capacity fails.
    #[inline]
    pub fn extend_from_slice(&mut self, extend: &[u8]) -> Result<(), AllocBytesError> {
        let cnt = extend.len();
        self.reserve(cnt)?;

        unsafe {
            // SAFETY:
            // - We assume that `AllocBytes::size` <= `AllocBytes::cap`.
            // - The source and destination cannot overlap if we own a mutable
            //   reference to the data, since rust's borrow checker disallows
            //   mutable references and non-mutable references to the same data.
            debug_assert!(
                self.0.size <= self.0.cap,
                "Size is greater than capacity in AllocBytesMut::extend_from_slice"
            );
            core::ptr::copy_nonoverlapping(
                extend.as_ptr(),
                self.0.ptr.as_ptr().add(self.0.size),
                cnt,
            );
            // SAFETY:
            // - We previously reserved `cnt` additional bytes, so it should
            //   have failed if the new size would overflow.
            debug_assert!(
                self.0.size <= usize::MAX - cnt,
                "Overflow in AllocBytesMut::extend_from_slice"
            );
            self.0.size = self.0.size.unchecked_add(cnt);
        }
        Ok(())
    }

    /// Freezes the mutable buffer, converting it into an immutable [`AllocBytes`].
    #[inline]
    pub fn freeze(self) -> AllocBytes<T, A, ALIGN> {
        self.0
    }

    /// Converts the mutable buffer into a [`Bytes`] object.
    ///
    /// Requires the `bytes` feature to be enabled.
    #[cfg(feature = "bytes")]
    #[inline]
    pub fn into_bytes(self) -> Bytes
    where
        T: 'static,
        A: 'static,
    {
        Bytes::from_owner(self.freeze())
    }
}
