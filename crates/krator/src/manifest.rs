use core::pin::Pin;
use core::task::{Context, Poll};
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio_stream::{wrappers::WatchStream, Stream};

/// Wrapper for `ObjectState::Manifest` type which reflects
/// the latest version of the object's manifest.
pub struct Manifest<T: Clone + Sync + Send + std::marker::Unpin + 'static> {
    rx: Receiver<T>,
    stream: WatchStream<T>,
}

impl<T: Clone + Sync + Send + std::marker::Unpin + 'static> Clone for Manifest<T> {
    fn clone(self: &Manifest<T>) -> Manifest<T> {
        Manifest {
            rx: self.rx.clone(),
            stream: WatchStream::new(self.rx.clone()),
        }
    }
}

impl<T: Clone + Sync + Send + std::marker::Unpin + 'static> Manifest<T> {
    /// Create a new Manifest wrapper from the initial object manifest.
    pub fn new(inner: T) -> (Sender<T>, Self) {
        let (tx, rx) = channel(inner);
        let stream = WatchStream::new(rx.clone());
        (tx, Manifest { rx, stream })
    }

    /// Obtain a clone of the latest object manifest.
    pub fn latest(&self) -> T {
        self.rx.borrow().clone()
    }
}

impl<T: Clone + Sync + Send + std::marker::Unpin + 'static + std::fmt::Debug> Stream
    for Manifest<T>
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio_stream::StreamExt;

    async fn watch_manifest(name: &str, mut m: Manifest<usize>) {
        while let Some(num) = m.next().await {
            println!("{} got update: {}", name, num);
        }
        println!("{} manifest closed.", name);
    }

    #[tokio::test]
    async fn test() {
        let (tx, manifest_1) = Manifest::new(0);
        let manifest_2 = manifest_1.clone();
        let manifest_3 = manifest_1.clone();

        let handle_1 = tokio::spawn(watch_manifest("manifest_1", manifest_1));
        let handle_2 = tokio::spawn(watch_manifest("manifest_2", manifest_2));
        let handle_3 = tokio::spawn(watch_manifest("manifest_3", manifest_3));
        for i in 1..5 {
            tx.send(i).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        drop(tx);
        handle_1.await.ok();
        handle_2.await.ok();
        handle_3.await.ok();
    }
}
