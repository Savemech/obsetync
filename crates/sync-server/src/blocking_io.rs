//! Small, explicitly bounded facade over Tokio's blocking executor.
//!
//! Acquiring a permit happens before `spawn_blocking`, so queued work remains
//! as cheap async futures rather than occupying an unbounded number of OS
//! threads. Storage and control-plane I/O use separate instances: a saturated
//! bulk read queue cannot starve ticket minting or an admin revocation.

use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct BlockingPool {
    name: &'static str,
    permits: Arc<Semaphore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockingError {
    Closed(&'static str),
    Join { pool: &'static str, message: String },
}

impl std::fmt::Display for BlockingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed(pool) => write!(formatter, "{pool} blocking pool is closed"),
            Self::Join { pool, message } => {
                write!(formatter, "{pool} blocking task failed: {message}")
            }
        }
    }
}

impl std::error::Error for BlockingError {}

impl BlockingPool {
    pub fn new(name: &'static str, parallelism: usize) -> Self {
        assert!(parallelism > 0, "blocking pool must have at least one slot");
        Self {
            name,
            permits: Arc::new(Semaphore::new(parallelism)),
        }
    }

    /// Run one coarse-grained blocking operation. Callers should submit a
    /// whole pack/page/scan rather than one closure per object.
    pub async fn run<F, T>(&self, operation: F) -> Result<T, BlockingError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| BlockingError::Closed(self.name))?;
        let name = self.name;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(|error| BlockingError::Join {
            pool: name,
            message: error.to_string(),
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

/// Reserve control-plane capacity independently of pack scans and reads.
pub fn control_pool() -> BlockingPool {
    BlockingPool::new("control I/O", 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn execution_never_exceeds_declared_parallelism() {
        let pool = BlockingPool::new("test", 2);
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let pool = pool.clone();
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            tasks.push(tokio::spawn(async move {
                pool.run(move || {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    maximum.fetch_max(now, Ordering::AcqRel);
                    std::thread::sleep(Duration::from_millis(5));
                    active.fetch_sub(1, Ordering::AcqRel);
                })
                .await
                .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(maximum.load(Ordering::Acquire), 2);
        assert_eq!(pool.available_permits(), 2);
    }

    #[tokio::test]
    async fn panic_is_reported_without_leaking_a_permit() {
        let pool = BlockingPool::new("test", 1);
        let error = pool
            .run(|| -> () { panic!("injected failure") })
            .await
            .unwrap_err();
        assert!(matches!(error, BlockingError::Join { pool: "test", .. }));
        assert_eq!(pool.available_permits(), 1);
        assert_eq!(pool.run(|| 42).await.unwrap(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn control_work_is_not_starved_by_saturated_storage_work() {
        let storage = BlockingPool::new("storage", 1);
        let control = BlockingPool::new("control", 1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let storage_task = tokio::spawn(async move {
            storage
                .run(move || {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                })
                .await
                .unwrap();
        });
        started_rx.await.unwrap();

        let control_result =
            tokio::time::timeout(Duration::from_millis(250), control.run(|| "ticket minted"))
                .await
                .expect("control pool was starved by unrelated storage work")
                .unwrap();
        assert_eq!(control_result, "ticket minted");

        release_tx.send(()).unwrap();
        storage_task.await.unwrap();
    }
}
