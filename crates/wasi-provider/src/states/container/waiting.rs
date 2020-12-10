use super::running::Running;
use super::{ContainerState, SharedContainerState};
use kubelet::container::state::prelude::*;

/// The container is starting.
#[derive(Default, Debug, TransitionTo)]
#[transition_to(Running)]
pub struct Waiting;

#[async_trait::async_trait]
impl State<SharedContainerState, ContainerState> for Waiting {
    async fn next(
        self: Box<Self>,
        _provider_state: SharedState<SharedContainerState>,
        _state: &mut ContainerState,
        _container: &Container,
    ) -> Transition<SharedContainerState, ContainerState> {
        todo!()
    }

    async fn status(
        &self,
        _state: &mut ContainerState,
        _container: &Container,
    ) -> anyhow::Result<Status> {
        Ok(Status::waiting("Module is starting."))
    }
}
