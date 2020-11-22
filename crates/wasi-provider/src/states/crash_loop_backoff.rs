use crate::{PodState, ProviderState};
use kubelet::backoff::BackoffStrategy;
use kubelet::state::prelude::*;

use super::registered::Registered;

#[derive(Default, Debug, TransitionTo)]
#[transition_to(Registered)]
pub struct CrashLoopBackoff;

#[async_trait::async_trait]
impl State<ProviderState, PodState> for CrashLoopBackoff {
    async fn next(
        self: Box<Self>,
        _provider_state: SharedState<ProviderState>,
        pod_state: &mut PodState,
        _pod: &Pod,
    ) -> Transition<ProviderState, PodState> {
        pod_state.crash_loop_backoff_strategy.wait().await;
        Transition::next(self, Registered)
    }

    async fn json_status(
        &self,
        _pod_state: &mut PodState,
        _pod: &Pod,
    ) -> anyhow::Result<serde_json::Value> {
        make_status(Phase::Pending, "CrashLoopBackoff")
    }
}
