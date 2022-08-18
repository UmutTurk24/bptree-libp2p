use futures::SinkExt;
use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use tokio::time::{sleep, Duration};

/// Sending a request to the main loop at every given interval
/// 
/// Refer to the declaration file to identify the meanings of each code
/// 
/// Returns event_receiver for the spawned thread
pub async fn topic_publisher (duration: u64) -> Receiver<i32> {
    let (mut event_sender, event_receiver) = mpsc::channel(0);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(duration)).await;
            event_sender.send(0);
        }
    });
    event_receiver
}

// Table of Code
// 0: Publish the block_map size