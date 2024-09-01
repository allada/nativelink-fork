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

use std::ops::Bound;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use async_lock::Mutex;
use async_trait::async_trait;
use futures::{future, stream, StreamExt, TryStreamExt};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueQualifier, ExecutionMetadata,
    OperationId, WorkerId,
};
use nativelink_util::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, OperationStageFlags, OrderDirection, WorkerStateManager,
};
use tokio::time::timeout;
use tracing::{event, Level};

use super::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedActionState,
};

/// Maximum number of times an update to the database
/// can fail before giving up.
const MAX_UPDATE_RETRIES: usize = 5;

/// Simple struct that implements the ActionStateResult trait and always returns an error.
struct ErrorActionStateResult(Error);

#[async_trait]
impl ActionStateResult for ErrorActionStateResult {
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        Err(self.0.clone())
    }

    async fn changed(&mut self) -> Result<Arc<ActionState>, Error> {
        Err(self.0.clone())
    }

    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error> {
        Err(self.0.clone())
    }
}

fn apply_filter_predicate(awaited_action: &AwaitedAction, filter: &OperationFilter) -> bool {
    // Note: The caller must filter `client_operation_id`.

    if let Some(operation_id) = &filter.operation_id {
        if operation_id != awaited_action.operation_id() {
            return false;
        }
    }

    if filter.worker_id.is_some() && filter.worker_id != awaited_action.worker_id() {
        return false;
    }

    {
        if let Some(filter_unique_key) = &filter.unique_key {
            match &awaited_action.action_info().unique_qualifier {
                ActionUniqueQualifier::Cachable(unique_key) => {
                    if filter_unique_key != unique_key {
                        return false;
                    }
                }
                ActionUniqueQualifier::Uncachable(_) => {
                    return false;
                }
            }
        }
        if let Some(action_digest) = filter.action_digest {
            if action_digest != awaited_action.action_info().digest() {
                return false;
            }
        }
    }

    {
        let last_worker_update_timestamp = awaited_action.last_worker_updated_timestamp();
        if let Some(worker_update_before) = filter.worker_update_before {
            if worker_update_before < last_worker_update_timestamp {
                return false;
            }
        }
        if let Some(completed_before) = filter.completed_before {
            if awaited_action.state().stage.is_finished()
                && completed_before < last_worker_update_timestamp
            {
                return false;
            }
        }
        if filter.stages != OperationStageFlags::Any {
            let stage_flag = match awaited_action.state().stage {
                ActionStage::Unknown => OperationStageFlags::Any,
                ActionStage::CacheCheck => OperationStageFlags::CacheCheck,
                ActionStage::Queued => OperationStageFlags::Queued,
                ActionStage::Executing => OperationStageFlags::Executing,
                ActionStage::Completed(_) => OperationStageFlags::Completed,
                ActionStage::CompletedFromCache(_) => OperationStageFlags::Completed,
            };
            if !filter.stages.intersects(stage_flag) {
                return false;
            }
        }
    }

    true
}

struct ClientActionStateResult<T, U: AwaitedActionDb> {
    inner: MatchingEngineActionStateResult<T, U>,
}

impl<T: AwaitedActionSubscriber, U: AwaitedActionDb> ClientActionStateResult<T, U> {
    fn new(sub: T, simple_scheduler_state_manager: Weak<SimpleSchedulerStateManager<U>>) -> Self {
        Self {
            inner: MatchingEngineActionStateResult::new(sub, simple_scheduler_state_manager),
        }
    }
}

#[async_trait]
impl<T: AwaitedActionSubscriber, U: AwaitedActionDb> ActionStateResult
    for ClientActionStateResult<T, U>
{
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        self.inner.as_state().await
    }

    async fn changed(&mut self) -> Result<Arc<ActionState>, Error> {
        self.inner.changed().await
    }

    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error> {
        self.inner.as_action_info().await
    }
}

struct MatchingEngineActionStateResult<T, U: AwaitedActionDb> {
    awaited_action_sub: T,
    simple_scheduler_state_manager: Weak<SimpleSchedulerStateManager<U>>,
}
impl<T: AwaitedActionSubscriber, U: AwaitedActionDb> MatchingEngineActionStateResult<T, U> {
    fn new(
        awaited_action_sub: T,
        simple_scheduler_state_manager: Weak<SimpleSchedulerStateManager<U>>,
    ) -> Self {
        Self {
            awaited_action_sub,
            simple_scheduler_state_manager,
        }
    }
}

#[async_trait]
impl<T: AwaitedActionSubscriber, U: AwaitedActionDb> ActionStateResult
    for MatchingEngineActionStateResult<T, U>
{
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        Ok(self.awaited_action_sub.borrow().state().clone())
    }

    async fn changed(&mut self) -> Result<Arc<ActionState>, Error> {
        let mut timeout_attempts = 0;
        loop {
            // TODO!(make const/config)
            let timeout_duration = Duration::from_secs(10);
            if let Ok(awaited_action_result) =
                timeout(timeout_duration, self.awaited_action_sub.changed()).await
            {
                return awaited_action_result
                    .err_tip(|| "In MatchingEngineActionStateResult::changed")
                    .map(|v| v.state().clone());
            }
            // AwaitedAction timed out. Check to see if the action is being executed
            // if it is then issue a retry.
            let awaited_action = self.awaited_action_sub.borrow();

            if !matches!(awaited_action.state().stage, ActionStage::Executing) {
                // We only timeout actions that are executing.
                // If they are queued or completed, we should not timeout.
                println!(
                    "operation not in executing state: {:?} - {}",
                    awaited_action.state().stage,
                    awaited_action.operation_id()
                );
                continue;
            }

            let simple_scheduler_state_manager = self
                .simple_scheduler_state_manager
                .upgrade()
                .err_tip(|| format!("Failed to upgrade weak reference to SimpleSchedulerStateManager in MatchingEngineActionStateResult::changed at attempt: {timeout_attempts}"))?;

            event!(
                Level::ERROR,
                ?awaited_action,
                "OperationId {} timed out after {} seconds issuing a retry",
                awaited_action.operation_id(),
                timeout_duration.as_secs_f32(),
            );

            println!("timing out operation id: {}", awaited_action.operation_id());
            simple_scheduler_state_manager
                .timeout_operation_id(awaited_action.operation_id())
                .await
                .err_tip(|| "In MatchingEngineActionStateResult::changed")?;
            println!(
                "done timing out operation id: {}",
                awaited_action.operation_id()
            );
            if timeout_attempts >= MAX_UPDATE_RETRIES {
                return Err(make_err!(
                    Code::Internal,
                    "Failed to update action after {} retries with no error set in MatchingEngineActionStateResult::changed",
                    MAX_UPDATE_RETRIES,
                ));
            }
            timeout_attempts += 1;
        }
    }

    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error> {
        Ok(self.awaited_action_sub.borrow().action_info().clone())
    }
}

/// SimpleSchedulerStateManager is responsible for maintaining the state of the scheduler.
/// Scheduler state includes the actions that are queued, active, and recently completed.
/// It also includes the workers that are available to execute actions based on allocation
/// strategy.
#[derive(MetricsComponent)]
pub struct SimpleSchedulerStateManager<T: AwaitedActionDb> {
    /// Database for storing the state of all actions.
    #[metric(group = "action_db")]
    action_db: T,

    /// Maximum number of times a job can be retried.
    // TODO(allada) This should be a scheduler decorator instead
    // of always having it on every SimpleScheduler.
    #[metric(help = "Maximum number of times a job can be retried")]
    max_job_retries: usize,

    timeout_operation_mux: Mutex<()>,

    /// Weak reference to self.
    weak_self: Weak<Self>,
}

impl<T: AwaitedActionDb> SimpleSchedulerStateManager<T> {
    pub fn new(max_job_retries: usize, action_db: T) -> Arc<Self> {
        Arc::new_cyclic(|weak_self| Self {
            action_db,
            max_job_retries,
            timeout_operation_mux: Mutex::new(()),
            weak_self: weak_self.clone(),
        })
    }

    /// Let the scheduler know that an operation has timed out from
    /// the client side (ie: worker has not updated in a while).
    async fn timeout_operation_id(&self, operation_id: &OperationId) -> Result<(), Error> {
        // Ensure that only one timeout operation is running at a time.
        // Failing to do this could result in the same operation being
        // timed out multiple times at the same time.
        let _lock = self.timeout_operation_mux.lock().await;

        let awaited_action_subscriber = self
            .action_db
            .get_by_operation_id(operation_id)
            .await
            .err_tip(|| "In SimpleSchedulerStateManager::timeout_operation_id")?
            .err_tip(|| {
                format!("Operation id {operation_id} does not exist in SimpleSchedulerStateManager::timeout_operation_id")
            })?;

        let awaited_action = awaited_action_subscriber.borrow();

        // If the action is not executing, we should not timeout the action.
        if !matches!(awaited_action.state().stage, ActionStage::Executing) {
            return Ok(());
        }

        // todo!(make this a config).
        let timeout_duration = Duration::from_secs(10);
        let timeout_operation_older_than = SystemTime::now()
            .checked_sub(timeout_duration)
            .err_tip(|| "Date too big in SimpleSchedulerStateManager::timeout_operation_id")?;
        if awaited_action.last_worker_updated_timestamp() > timeout_operation_older_than {
            // The action was updated recently, we should not timeout the action.
            // This is to prevent timing out actions that have recently been updated
            // (like multiple clients timeout the same action at the same time).
            return Ok(());
        }

        self.assign_operation(
            operation_id,
            Err(make_err!(
                Code::DeadlineExceeded,
                "Operation timed out after {} seconds",
                timeout_duration.as_secs_f32(),
            )),
        )
        .await
    }

    async fn inner_update_operation(
        &self,
        operation_id: &OperationId,
        maybe_worker_id: Option<&WorkerId>,
        action_stage_result: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let mut last_err = None;
        for _ in 0..MAX_UPDATE_RETRIES {
            let maybe_awaited_action_subscriber = self
                .action_db
                .get_by_operation_id(operation_id)
                .await
                .err_tip(|| "In SimpleSchedulerStateManager::update_operation")?;
            let awaited_action_subscriber = match maybe_awaited_action_subscriber {
                Some(sub) => sub,
                // No action found. It is ok if the action was not found. It probably
                // means that the action was dropped, but worker was still processing
                // it.
                None => return Ok(()),
            };

            let mut awaited_action = awaited_action_subscriber.borrow();

            // Make sure the worker id matches the awaited action worker id.
            // This might happen if the worker sending the update is not the
            // worker that was assigned.
            if awaited_action.worker_id().is_some()
                && maybe_worker_id.is_some()
                && maybe_worker_id != awaited_action.worker_id().as_ref()
            {
                // If another worker is already assigned to the action, another
                // worker probably picked up the action. We should not update the
                // action in this case and abort this operation.
                let err = make_err!(
                    Code::Aborted,
                    "Worker ids do not match - {:?} != {:?} for {:?}",
                    maybe_worker_id,
                    awaited_action.worker_id(),
                    awaited_action,
                );
                event!(
                    Level::INFO,
                    "Worker ids do not match - {:?} != {:?} for {:?}. This is probably due to another worker picking up the action.",
                    maybe_worker_id,
                    awaited_action.worker_id(),
                    awaited_action,
                );
                return Err(err);
            }

            let stage = match &action_stage_result {
                Ok(stage) => stage.clone(),
                Err(err) => {
                    // Don't count a backpressure failure as an attempt for an action.
                    let due_to_backpressure = err.code == Code::ResourceExhausted;
                    if !due_to_backpressure {
                        awaited_action.attempts += 1;
                    }

                    if awaited_action.attempts > self.max_job_retries {
                        ActionStage::Completed(ActionResult {
                            execution_metadata: ExecutionMetadata {
                                worker: maybe_worker_id.map_or_else(String::default, |v| v.to_string()),
                                ..ExecutionMetadata::default()
                            },
                            error: Some(err.clone().merge(make_err!(
                                Code::Internal,
                                "Job cancelled because it attempted to execute too many times and failed {}",
                                format!("for operation_id: {operation_id}, maybe_worker_id: {maybe_worker_id:?}"),
                            ))),
                            ..ActionResult::default()
                        })
                    } else {
                        ActionStage::Queued
                    }
                }
            };
            if matches!(stage, ActionStage::Queued) {
                // If the action is queued, we need to unset the worker id regardless of
                // which worker sent the update.
                awaited_action.set_worker_id(None);
            } else {
                awaited_action.set_worker_id(maybe_worker_id.copied());
            }
            awaited_action.set_state(Arc::new(ActionState {
                stage,
                operation_id: operation_id.clone(),
                action_digest: awaited_action.action_info().digest(),
            }));

            let update_action_result = self
                .action_db
                .update_awaited_action(awaited_action)
                .await
                .err_tip(|| "In SimpleSchedulerStateManager::update_operation");
            if let Err(err) = update_action_result {
                // We use Aborted to signal that the action was not
                // updated due to the data being set was not the latest
                // but can be retried.
                if err.code == Code::Aborted {
                    last_err = Some(err);
                    continue;
                } else {
                    return Err(err);
                }
            }
            return Ok(());
        }
        match last_err {
            Some(err) => Err(err),
            None => Err(make_err!(
                Code::Internal,
                "Failed to update action after {} retries with no error set",
                MAX_UPDATE_RETRIES,
            )),
        }
    }

    async fn inner_add_operation(
        &self,
        new_client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<T::Subscriber, Error> {
        self.action_db
            .add_action(new_client_operation_id, action_info)
            .await
            .err_tip(|| "In SimpleSchedulerStateManager::add_operation")
    }

    async fn inner_filter_operations<'a, F>(
        &'a self,
        filter: OperationFilter,
        to_action_state_result: F,
    ) -> Result<ActionStateResultStream<'a>, Error>
    where
        F: Fn(T::Subscriber) -> Box<dyn ActionStateResult> + Send + Sync + 'a,
    {
        fn sorted_awaited_action_state_for_flags(
            stage: OperationStageFlags,
        ) -> Option<SortedAwaitedActionState> {
            match stage {
                OperationStageFlags::CacheCheck => Some(SortedAwaitedActionState::CacheCheck),
                OperationStageFlags::Queued => Some(SortedAwaitedActionState::Queued),
                OperationStageFlags::Executing => Some(SortedAwaitedActionState::Executing),
                OperationStageFlags::Completed => Some(SortedAwaitedActionState::Completed),
                _ => None,
            }
        }

        if let Some(operation_id) = &filter.operation_id {
            return Ok(self
                .action_db
                .get_by_operation_id(operation_id)
                .await
                .err_tip(|| "In MemorySchedulerStateManager::filter_operations")?
                .filter(|awaited_action_rx| {
                    let awaited_action = awaited_action_rx.borrow();
                    apply_filter_predicate(&awaited_action, &filter)
                })
                .map(|awaited_action| -> ActionStateResultStream {
                    Box::pin(stream::once(async move {
                        to_action_state_result(awaited_action)
                    }))
                })
                .unwrap_or_else(|| Box::pin(stream::empty())));
        }
        if let Some(client_operation_id) = &filter.client_operation_id {
            return Ok(self
                .action_db
                .get_awaited_action_by_id(client_operation_id)
                .await
                .err_tip(|| "In MemorySchedulerStateManager::filter_operations")?
                .filter(|awaited_action_rx| {
                    let awaited_action = awaited_action_rx.borrow();
                    apply_filter_predicate(&awaited_action, &filter)
                })
                .map(|awaited_action| -> ActionStateResultStream {
                    Box::pin(stream::once(async move {
                        to_action_state_result(awaited_action)
                    }))
                })
                .unwrap_or_else(|| Box::pin(stream::empty())));
        }

        let Some(sorted_awaited_action_state) =
            sorted_awaited_action_state_for_flags(filter.stages)
        else {
            let mut all_items: Vec<T::Subscriber> = self
                .action_db
                .get_all_awaited_actions()
                .await
                .try_filter(|awaited_action_subscriber| {
                    future::ready(apply_filter_predicate(
                        &awaited_action_subscriber.borrow(),
                        &filter,
                    ))
                })
                .try_collect()
                .await
                .err_tip(|| "In MemorySchedulerStateManager::filter_operations")?;
            match filter.order_by_priority_direction {
                Some(OrderDirection::Asc) => {
                    all_items.sort_unstable_by_key(|a| a.borrow().sort_key())
                }
                Some(OrderDirection::Desc) => {
                    all_items.sort_unstable_by_key(|a| std::cmp::Reverse(a.borrow().sort_key()))
                }
                None => {}
            }
            return Ok(Box::pin(stream::iter(
                all_items.into_iter().map(to_action_state_result),
            )));
        };

        let desc = matches!(
            filter.order_by_priority_direction,
            Some(OrderDirection::Desc)
        );
        let filter = filter.clone();
        let stream = self
            .action_db
            .get_range_of_actions(
                sorted_awaited_action_state,
                Bound::Unbounded,
                Bound::Unbounded,
                desc,
            )
            .await
            .try_filter(move |sub| future::ready(apply_filter_predicate(&sub.borrow(), &filter)))
            .map(move |result| -> Box<dyn ActionStateResult> {
                result.map_or_else(
                    |e| -> Box<dyn ActionStateResult> { Box::new(ErrorActionStateResult(e)) },
                    |v| -> Box<dyn ActionStateResult> { to_action_state_result(v) },
                )
            });
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<T: AwaitedActionDb> ClientStateManager for SimpleSchedulerStateManager<T> {
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        let sub = self
            .inner_add_operation(client_operation_id.clone(), action_info.clone())
            .await?;

        Ok(Box::new(ClientActionStateResult::new(
            sub,
            self.weak_self.clone(),
        )))
    }

    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error> {
        self.inner_filter_operations(filter, move |rx| {
            Box::new(ClientActionStateResult::new(rx, self.weak_self.clone()))
        })
        .await
    }

    fn as_known_platform_property_provider(&self) -> Option<&dyn KnownPlatformPropertyProvider> {
        None
    }
}

#[async_trait]
impl<T: AwaitedActionDb> WorkerStateManager for SimpleSchedulerStateManager<T> {
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        action_stage_result: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        self.inner_update_operation(operation_id, Some(worker_id), action_stage_result)
            .await
    }
}

#[async_trait]
impl<T: AwaitedActionDb> MatchingEngineStateManager for SimpleSchedulerStateManager<T> {
    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error> {
        self.inner_filter_operations(filter, |rx| {
            Box::new(MatchingEngineActionStateResult::new(
                rx,
                self.weak_self.clone(),
            ))
        })
        .await
    }

    async fn assign_operation(
        &self,
        operation_id: &OperationId,
        worker_id_or_reason_for_unsassign: Result<&WorkerId, Error>,
    ) -> Result<(), Error> {
        let (maybe_worker_id, stage_result) = match worker_id_or_reason_for_unsassign {
            Ok(worker_id) => (Some(worker_id), Ok(ActionStage::Executing)),
            Err(err) => (None, Err(err)),
        };
        self.inner_update_operation(operation_id, maybe_worker_id, stage_result)
            .await
    }
}
