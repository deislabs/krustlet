use super::image_pull::ImagePull;
use crate::{PodState, ProviderState};
use kubelet::backoff::BackoffStrategy;
use kubelet::state::prelude::*;

/// Kubelet encountered an error when pulling container image.
#[derive(Default, Debug, TransitionTo)]
#[transition_to(ImagePull)]
pub struct ImagePullBackoff;

#[async_trait::async_trait]
impl State<ProviderState, PodState> for ImagePullBackoff {
    async fn next(
        self: Box<Self>,
        _provider_state: SharedState<ProviderState>,
        pod_state: &mut PodState,
        _pod: &Pod,
    ) -> Transition<ProviderState, PodState> {
        pod_state.image_pull_backoff_strategy.wait().await;
        Transition::next(self, ImagePull)
    }

    async fn json_status(
        &self,
        _pod_state: &mut PodState,
        _pod: &Pod,
    ) -> anyhow::Result<serde_json::Value> {
        make_status(Phase::Pending, "ImagePullBackoff")
    }
}
