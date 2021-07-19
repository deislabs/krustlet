use kubelet::container::state::prelude::*;
use tracing::{error, instrument};

use crate::ProviderState;

use super::ContainerState;

/// The container is starting.
#[derive(Debug, TransitionTo)]
#[transition_to()]
pub struct Terminated {
    message: String,
    failed: bool,
}

impl Terminated {
    pub fn new(message: String, failed: bool) -> Self {
        Terminated { message, failed }
    }
}

#[async_trait::async_trait]
impl State<ContainerState> for Terminated {
    #[instrument(level = "info", skip(self, _shared_state, _state, container), fields(pod_name = _state.pod.name(), container_name))]
    async fn next(
        self: Box<Self>,
        _shared_state: SharedState<ProviderState>,
        _state: &mut ContainerState,
        container: Manifest<Container>,
    ) -> Transition<ContainerState> {
        let container = container.latest();

        tracing::Span::current().record("container_name", &container.name());

        if self.failed {
            error!(
                error = %self.message,
                "Pod container exited with error"
            );
            Transition::Complete(Err(anyhow::anyhow!(self.message.clone())))
        } else {
            Transition::Complete(Ok(()))
        }
    }

    async fn status(
        &self,
        _state: &mut ContainerState,
        _container: &Container,
    ) -> anyhow::Result<Status> {
        Ok(Status::terminated(&self.message, self.failed))
    }
}
