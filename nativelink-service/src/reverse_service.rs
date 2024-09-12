// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::{Request, Response};
use nativelink_config::cas_server::{InstanceName, WorkerApiConfig};
use nativelink_error::Error;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::task::TaskExecutor;
use reverse_service::reverse_service::{ClientService, RootService};
use tower::Service;

// type InstanceInfoName = String;

pub struct ReverseService {
    _instance_infos: HashMap<InstanceName, Arc<dyn WorkerScheduler>>,
}

impl ReverseService {
    pub fn new(
        _config: &WorkerApiConfig,
        _schedulers: &HashMap<String, Arc<dyn WorkerScheduler>>,
    ) -> Result<Self, Error> {
        // let mut instance_infos = HashMap::with_capacity(config.len());
        // for (instance_name, exec_cfg) in config {
        //     let cas_store = store_manager
        //         .get_store(&exec_cfg.cas_store)
        //         .ok_or_else(|| {
        //             make_input_err!("'cas_store': '{}' does not exist", exec_cfg.cas_store)
        //         })?;
        //     let scheduler = scheduler_map
        //         .get(&exec_cfg.scheduler)
        //         .err_tip(|| {
        //             format!(
        //                 "Scheduler needs config for '{}' because it exists in execution",
        //                 exec_cfg.scheduler
        //             )
        //         })?
        //         .clone();

        //     instance_infos.insert(
        //         instance_name.to_string(),
        //         InstanceInfo {
        //             scheduler,
        //             cas_store,
        //         },
        //     );
        // }
        // Ok(Self { instance_infos })
        Ok(Self {
            _instance_infos: HashMap::new(),
        })
    }

    // pub fn into_service(self) -> Arc<RootService<TaskExecutor, impl Fn(ClientService)>> {
    pub fn into_service(self) -> RootService<TaskExecutor, impl Fn(ClientService) + Send + Clone> {
        let h2_client_builder = hyper::client::conn::http2::Builder::new(TaskExecutor::new());
        let client_handler = move |_client_service| {
            println!("Client service received");
        };
        RootService::new(h2_client_builder, client_handler)
        // axum::routing::get(move |request: hyper::Request<axum::body::Body>| async move {
        //     println!("Request: {:?}", request);
        //     Response::new(axum::body::Body::from("fooo".to_string()))
        // })
    }
}

