use crate::coordinator::{CoordinatorMsg, CoordinatorTx};
use crate::discovery::{DeviceIdentity, enumerate_identities, is_event_node, open_identity};
use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, info, warn};

const INPUT_DIR: &str = "/dev/input";

/// Watch `/dev/input` for input devices. Existing nodes are picked up
/// in an initial sweep; later additions are detected via inotify. Each
/// observed `DeviceIdentity` is forwarded to the coordinator, which
/// decides whether to monitor it.
///
/// `shutdown` is awaited concurrently; when it resolves, the watcher
/// returns.
pub async fn run_watcher<F>(coordinator: CoordinatorTx, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let inotify = Inotify::init()?;
    inotify
        .watches()
        .add(INPUT_DIR, WatchMask::CREATE | WatchMask::ATTRIB)?;

    for identity in enumerate_identities() {
        forward(&coordinator, identity);
    }

    let mut buffer = [0u8; 4096];
    let mut events = inotify.into_event_stream(&mut buffer[..])?;
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; stopping device watcher");
                return Ok(());
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(ev)) => {
                        if let Some(path) = event_path(ev.name.as_deref())
                            && let Some(identity) = open_with_retry(&path).await
                        {
                            forward(&coordinator, identity);
                        }
                    }
                    Some(Err(e)) => warn!(error = %e, "inotify read error"),
                    None => {
                        warn!("inotify stream ended unexpectedly");
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn forward(coordinator: &CoordinatorTx, identity: DeviceIdentity) {
    if !identity.has_keys {
        debug!(
            path = %identity.path.display(),
            name = %identity.name,
            "skipping device with no key support",
        );
        return;
    }
    if coordinator
        .send(CoordinatorMsg::DeviceConnected(identity))
        .is_err()
    {
        debug!("coordinator channel closed; cannot forward device");
    }
}

async fn open_with_retry(path: &Path) -> Option<DeviceIdentity> {
    if let Some(id) = open_identity(path) {
        return Some(id);
    }
    // Ride out the CREATE → chown race on hotplug.
    tokio::time::sleep(Duration::from_millis(50)).await;
    open_identity(path)
}

fn event_path(name: Option<&OsStr>) -> Option<PathBuf> {
    let raw = name?.to_str()?;
    if is_event_node(raw) {
        Some(PathBuf::from(INPUT_DIR).join(raw))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_path_builds_full_path() {
        assert_eq!(
            event_path(Some(OsStr::new("event3"))),
            Some(PathBuf::from("/dev/input/event3"))
        );
    }

    #[test]
    fn event_path_skips_non_event_nodes() {
        assert_eq!(event_path(Some(OsStr::new("mice"))), None);
        assert_eq!(event_path(None), None);
    }
}
