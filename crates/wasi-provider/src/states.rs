pub(crate) mod completed;
pub(crate) mod initializing;
pub(crate) mod running;
pub(crate) mod starting;
pub(crate) mod terminated;

/// When called in a state's `next` function, exits the current state
/// and transitions to the Error state.
#[macro_export]
macro_rules! transition_to_error {
    ($slf:ident, $err:ident) => {{
        let aerr = anyhow::Error::from($err);
        log::error!("{:?}", aerr);
        let error_state =
            kubelet::state::common::error::Error::<crate::WasiProvider>::new(aerr.to_string());
        return Transition::next($slf, error_state);
    }};
}

/// When called in a state's `next` function, exits the state machine
/// returns a fatal error to the kubelet.
#[macro_export]
macro_rules! fail_fatal {
    ($err:ident) => {{
        let aerr = anyhow::Error::from($err);
        log::error!("{:?}", aerr);
        return Transition::Complete(Err(aerr));
    }};
}
