//! Object-store collection that no writer owns: part objects no manifest
//! names any more, and catalog history no startup reads any more.
//!
//! It runs on its own cadence rather than inside retention, because retention
//! only reaches the store when it retires a part, and merges and compactions
//! leave orphans on every pass whether or not anything has expired.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use crate::config::Config;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::RemoteCache;
use crate::shutdown::wait_for_drain;

pub async fn object_store_gc_loop(
    cache: Arc<RemoteCache>,
    config: Arc<Config>,
    metrics: Arc<RuntimeMetrics>,
    mut drain_rx: watch::Receiver<bool>,
) {
    let mut ticker = interval(config.orphan_gc_interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = wait_for_drain(&mut drain_rx) => return,
        }
        match object_store_gc_once(&cache, &config, &metrics).await {
            Ok(()) => {
                cache.record_remote_success();
                metrics
                    .object_store_gc_success
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                cache.record_remote_failure();
                metrics
                    .object_store_gc_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(%error, "object-store collection failed");
            }
        }
    }
}

async fn object_store_gc_once(
    cache: &RemoteCache,
    config: &Config,
    metrics: &RuntimeMetrics,
) -> Result<(), String> {
    let orphans_removed = tokio::time::timeout(
        config.max_retention_runtime,
        cache
            .storage
            .garbage_collect_orphans(config.retention_grace_period),
    )
    .await
    .map_err(|_| "orphan collection timed out".to_string())??;
    metrics
        .orphan_objects_removed
        .fetch_add(orphans_removed as u64, Ordering::Relaxed);

    let Some(min_age) = config.catalog_prune_min_age else {
        return Ok(());
    };
    let catalog_objects_pruned = tokio::time::timeout(
        config.max_retention_runtime,
        cache.storage.prune_catalog(min_age),
    )
    .await
    .map_err(|_| "catalog pruning timed out".to_string())??;
    metrics
        .catalog_objects_pruned
        .fetch_add(catalog_objects_pruned as u64, Ordering::Relaxed);
    if orphans_removed > 0 || catalog_objects_pruned > 0 {
        tracing::info!(
            orphans_removed,
            catalog_objects_pruned,
            "object-store collection removed objects"
        );
    }
    Ok(())
}
