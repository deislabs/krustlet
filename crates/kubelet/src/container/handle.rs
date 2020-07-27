use std::io::SeekFrom;

use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio::sync::watch::Receiver;

use crate::container::{Container, ContainerMap, KubeStatusInfo};
use crate::handle::StopHandler;
use crate::log::{stream, HandleFactory, Sender};

/// Represents a "container" (whatever that might be) instance at runtime. This
/// can be used on its own, however, it is generally better to use it as a part
/// of a [`pod::Handle`], which manages a group of containers in a Kubernetes
/// Pod
pub struct RuntimeContainer<H, F> {
    spec: Container,
    handle: H,
    handle_factory: F,
    status_channel: Receiver<KubeStatusInfo>,
}

impl<H: StopHandler, F> RuntimeContainer<H, F> {
    /// Create a new runtime with the given handle for stopping the runtime,
    /// a reader for log output, and a status channel.
    ///
    /// The status channel is a [Tokio watch `Receiver`][Receiver]. The sender part
    /// of the channel should be given to the running process and the receiver half
    /// passed to this constructor to be used for reporting current status
    pub fn new(
        spec: Container,
        handle: H,
        handle_factory: F,
        status_channel: Receiver<KubeStatusInfo>,
    ) -> Self {
        Self {
            spec,
            handle,
            handle_factory,
            status_channel,
        }
    }

    /// Signal the running instance to stop. Use [`Handle::wait`] to wait for the process to
    /// exit. This uses the underlying [`StopHandler`] implementation passed to the constructor
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        self.handle.stop().await
    }

    /// Streams output from the running process into the given sender.
    /// Optionally tails the output and/or continues to watch the file and stream changes.
    pub(crate) async fn output<R>(&mut self, sender: Sender) -> anyhow::Result<()>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
        F: HandleFactory<R>,
    {
        let mut handle = self.handle_factory.new_handle();
        handle.seek(SeekFrom::Start(0)).await?;
        tokio::spawn(stream(handle, sender));
        Ok(())
    }

    /// Returns a clone of the status_channel for use in reporting the status to
    /// another process
    pub fn status(&self) -> Receiver<KubeStatusInfo> {
        self.status_channel.clone()
    }

    /// Wait for the running process to complete. Generally speaking,
    /// [`Handle::stop`] should be called first. This uses the underlying
    /// [`StopHandler`] implementation passed to the constructor
    pub(crate) async fn wait(&mut self) -> anyhow::Result<()> {
        self.handle.wait().await
    }

    pub(crate) fn spec(&self) -> Container {
        self.spec.clone()
    }
}

/// A map from containers to container handles.
pub type HandleMap<H, F> = ContainerMap<RuntimeContainer<H, F>>;
