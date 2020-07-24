use std::io::SeekFrom;

use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio::sync::watch::Receiver;

use crate::container::{ContainerMap, Status};
use crate::handle::StopHandler;
use crate::log::{stream, LogReaderFactory, Sender};

/// Represents a handle to a running "container" (whatever that might be). This
/// can be used on its own, however, it is generally better to use it as a part
/// of a [`pod::Handle`], which manages a group of containers in a Kubernetes
/// Pod
pub struct Handle<S, F> {
    stopper: S,
    log_reader_factory: F,
    status_channel: Receiver<Status>,
}

impl<S: StopHandler, F> Handle<S, F> {
    /// Create a new runtime with the given handle for stopping the runtime,
    /// a reader for log output, and a status channel.
    ///
    /// The status channel is a [Tokio watch `Receiver`][Receiver]. The sender part
    /// of the channel should be given to the running process and the receiver half
    /// passed to this constructor to be used for reporting current status
    pub fn new(stopper: S, log_reader_factory: F, status_channel: Receiver<Status>) -> Self {
        Self {
            stopper,
            log_reader_factory,
            status_channel,
        }
    }

    /// Signal the running instance to stop. Use [`Handle::wait`] to wait for the process to
    /// exit. This uses the underlying [`StopHandler`] implementation passed to the constructor
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        self.stopper.stop().await
    }

    /// Streams output from the running process into the given sender.
    /// Optionally tails the output and/or continues to watch the file and stream changes.
    pub(crate) async fn output(&mut self, sender: Sender) -> anyhow::Result<()>
    where
        F: LogReaderFactory,
        F::Reader: AsyncRead + AsyncSeek + Unpin + Send + 'static,
    {
        let mut reader = self.log_reader_factory.new_reader();
        reader.seek(SeekFrom::Start(0)).await?;
        tokio::spawn(stream(reader, sender));
        Ok(())
    }

    /// Returns a clone of the status_channel for use in reporting the status to
    /// another process
    pub fn status(&self) -> Receiver<Status> {
        self.status_channel.clone()
    }

    /// Wait for the running process to complete. Generally speaking,
    /// [`Handle::stop`] should be called first. This uses the underlying
    /// [`StopHandler`] implementation passed to the constructor
    pub(crate) async fn wait(&mut self) -> anyhow::Result<()> {
        self.stopper.wait().await
    }
}

/// A map from containers to container handles.
pub type HandleMap<H, F> = ContainerMap<Handle<H, F>>;
