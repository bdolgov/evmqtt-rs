use crate::coordinator::{CoordinatorMsg, CoordinatorTx};
use crate::discovery::{DeviceIdentity, is_event_node, list_event_paths, try_open_identity};
use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

const INPUT_DIR: &str = "/dev/input";
const OPEN_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const OPEN_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Watch `/dev/input` for input devices. Existing nodes are picked up
/// in an initial sweep; later additions are detected via inotify. Each
/// observed `DeviceIdentity` is forwarded to the coordinator, which
/// decides whether to monitor it.
///
/// Opening a freshly-appeared `eventN` node can fail for a while: udev
/// hasn't run chown yet, or (in a Home Assistant addon) the cgroup
/// device controller hasn't been updated to grant us access. We don't
/// know which case we're in, so for every newly-seen path we spawn a
/// background retry that tries every second for 30 seconds before
/// giving up.
///
/// `shutdown` is awaited concurrently; when it resolves, the watcher
/// returns and any outstanding retry tasks are aborted.
pub async fn run_watcher<F>(coordinator: CoordinatorTx, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let inotify = Inotify::init()?;
    inotify
        .watches()
        .add(INPUT_DIR, WatchMask::CREATE | WatchMask::ATTRIB)?;

    let mut retries: JoinSet<()> = JoinSet::new();
    for path in list_event_paths() {
        spawn_open_retry(&mut retries, coordinator.clone(), path);
    }

    let mut buffer = [0u8; 4096];
    let mut events = inotify.into_event_stream(&mut buffer[..])?;
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; stopping device watcher");
                retries.abort_all();
                return Ok(());
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(ev)) => {
                        if let Some(path) = event_path(ev.name.as_deref()) {
                            spawn_open_retry(&mut retries, coordinator.clone(), path);
                        }
                    }
                    Some(Err(e)) => warn!(error = %e, "inotify read error"),
                    None => {
                        warn!("inotify stream ended unexpectedly");
                        retries.abort_all();
                        return Ok(());
                    }
                }
            }
            Some(_) = retries.join_next(), if !retries.is_empty() => {
                // Reap finished retry tasks so the set doesn't grow.
            }
        }
    }
}

fn spawn_open_retry(retries: &mut JoinSet<()>, coordinator: CoordinatorTx, path: PathBuf) {
    info!(path = %path.display(), "input device detected; attempting to open for up to 30s");
    retries.spawn(async move {
        open_with_retry(&coordinator, &path).await;
    });
}

async fn open_with_retry(coordinator: &CoordinatorTx, path: &Path) {
    let deadline = Instant::now() + OPEN_RETRY_TIMEOUT;
    let last_err = loop {
        let err = match try_open_identity(path) {
            Ok(identity) => {
                info!(path = %path.display(), name = %identity.name, "opened input device");
                forward(coordinator, identity);
                return;
            }
            Err(e) => e,
        };
        if Instant::now() >= deadline {
            break err;
        }
        tokio::time::sleep(OPEN_RETRY_INTERVAL).await;
    };
    warn!(
        path = %path.display(),
        error = %last_err,
        "gave up opening device after 30s",
    );
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
