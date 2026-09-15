//! The trace compactor: size-tiered, per partition, on the metric
//! compactor's crash-safety shape.
//!
//! Every flush writes one trace part per partition, so a steady trickle of
//! spans becomes one part per flush interval, each a Parquet file, a bloom
//! file and a metadata file locally and in the object store. The compactor
//! rewrites a full tier into one part, which is what keeps the part count, and
//! with it the object count, the catalog size and the per-query open cost,
//! growing with the data rather than with the number of flushes.
//!
//! A pass holds the deletion lock's read half from input selection to the
//! visibility commit, as the log merge does, so neither retention nor cache
//! eviction can remove an input body it is reading. Missing bodies are
//! restored first, because eviction may already have dropped them.
//!
//! **Crash safety is a commit record**, `traces_root/.compact/<id>.json`,
//! written after the replacement is durable and before any input is removed.
//! Local mode resolves it before the registry loads; remote mode replays it in
//! `reconcile_trace_local_cache`, where the manifest replacement is
//! idempotent. Input objects are left to the orphan collector rather than
//! deleted here, so a query that planned against an input before the
//! replacement landed can still restore it for the length of the grace period.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::interval;

use crate::config::Config;
use crate::object_storage::{RemoteCache, TraceManifestPart, is_inputs_changed_error};
use crate::shutdown::wait_for_drain;
use crate::trace_part::{self, TracePartReader};
use crate::trace_registry::TraceRegistry;

pub const COMPACT_MIN_PARTS: usize = 8;
const COMPACT_MAX_PARTS: usize = 32;
/// Stored bytes one pass reads. A decoded span is several times its stored
/// size, so this is what bounds the pass's memory rather than the part count.
const COMPACT_MAX_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const L0_MAX_BYTES: u64 = 1024 * 1024;
const L1_MAX_BYTES: u64 = 16 * 1024 * 1024;
const COMPACT_DIR: &str = ".compact";
const SPAN_DECODE_EXPANSION: u64 = 8;

/// Durable intent: the replacement is in `new`, the inputs it supersedes in
/// `inputs`, both as `partition/id` relative to the traces root.
#[derive(Serialize, Deserialize)]
pub(crate) struct CompactRecord {
    pub new: Vec<String>,
    pub inputs: Vec<String>,
}

fn stored_bytes(reader: &TracePartReader) -> u64 {
    reader
        .part()
        .meta
        .tenants
        .iter()
        .map(|segment| segment.bytes.len())
        .sum()
}

/// `None` for a part already at the largest tier: rewriting large parts
/// together costs a full read for no reduction in part count that matters.
fn tier_of(bytes: u64) -> Option<u8> {
    if bytes < L0_MAX_BYTES {
        Some(0)
    } else if bytes < L1_MAX_BYTES {
        Some(1)
    } else {
        None
    }
}

/// The first (partition, tier) holding at least [`COMPACT_MIN_PARTS`] parts,
/// smallest first, cut at [`COMPACT_MAX_PARTS`] and [`COMPACT_MAX_INPUT_BYTES`].
pub(crate) fn select_inputs(readers: &[Arc<TracePartReader>]) -> Option<Vec<Arc<TracePartReader>>> {
    let mut groups: BTreeMap<(String, u8), Vec<Arc<TracePartReader>>> = BTreeMap::new();
    for reader in readers {
        let Some(tier) = tier_of(stored_bytes(reader)) else {
            continue;
        };
        groups
            .entry((reader.part().meta.partition.clone(), tier))
            .or_default()
            .push(reader.clone());
    }
    for (_, mut group) in groups {
        if group.len() < COMPACT_MIN_PARTS {
            continue;
        }
        group.sort_by_key(|reader| stored_bytes(reader));
        let mut selected = Vec::new();
        let mut selected_bytes = 0u64;
        for reader in group {
            let bytes = stored_bytes(&reader);
            if selected.len() == COMPACT_MAX_PARTS
                || (!selected.is_empty() && selected_bytes + bytes > COMPACT_MAX_INPUT_BYTES)
            {
                break;
            }
            selected_bytes += bytes;
            selected.push(reader);
        }
        if selected.len() >= 2 {
            return Some(selected);
        }
    }
    None
}

pub(crate) fn compact_dir(traces_root: &Path) -> PathBuf {
    traces_root.join(COMPACT_DIR)
}

fn relative_dir(traces_root: &Path, dir: &Path) -> Result<String, String> {
    let relative = dir
        .strip_prefix(traces_root)
        .map_err(|_| format!("part directory escapes the traces root: {}", dir.display()))?;
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("part directory is not UTF-8: {}", dir.display()))
}

fn write_record(traces_root: &Path, id: &str, record: &CompactRecord) -> Result<PathBuf, String> {
    let dir = compact_dir(traces_root);
    std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    let path = dir.join(format!("{id}.json"));
    let bytes = serde_json::to_vec_pretty(record).map_err(|error| error.to_string())?;
    std::fs::write(&path, bytes).map_err(|error| error.to_string())?;
    std::fs::File::open(&path)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    std::fs::File::open(&dir)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(path)
}

pub(crate) fn remove_record(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

/// Every pending commit record, with its path. A malformed record is an error
/// rather than a skip: dropping one could leave inputs alive beside their
/// replacement, and every span in them would answer twice.
pub(crate) fn read_records(traces_root: &Path) -> Result<Vec<(PathBuf, CompactRecord)>, String> {
    let dir = compact_dir(traces_root);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.to_string()),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|error| error.to_string())?;
        let record: CompactRecord = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "invalid trace compaction record {}: {error}",
                path.display()
            )
        })?;
        records.push((path, record));
    }
    Ok(records)
}

/// A record's relative dir joined back under the root, refusing traversal.
pub(crate) fn record_dir(traces_root: &Path, relative: &str) -> Result<PathBuf, String> {
    let path = Path::new(relative);
    let mut components = path.components();
    let safe = matches!(
        (components.next(), components.next(), components.next()),
        (
            Some(std::path::Component::Normal(_)),
            Some(std::path::Component::Normal(_)),
            None
        )
    );
    if !safe {
        return Err(format!("unsafe trace compaction record path {relative:?}"));
    }
    Ok(traces_root.join(path))
}

/// The input of a record as a manifest descriptor.
pub(crate) fn record_input_descriptor(relative: &str) -> Result<TraceManifestPart, String> {
    let (partition, id) = relative
        .split_once('/')
        .ok_or_else(|| format!("malformed trace compaction input {relative:?}"))?;
    Ok(TraceManifestPart {
        id: id.to_string(),
        partition: partition.to_string(),
    })
}

/// Local-mode crash recovery, run before the registry loads: a record whose
/// replacement is durable wins and its surviving inputs are removed; a record
/// whose replacement never became durable loses and only the record goes.
///
/// Remote mode must not run this. There the manifest may still name the
/// inputs, and removing them locally first would leave the reconcile scan
/// publishing the replacement beside inputs it then restores again.
pub fn recover_local_compactions(traces_root: &Path) -> Result<(), String> {
    for (path, record) in read_records(traces_root)? {
        let replacement_durable = record.new.iter().all(|relative| {
            record_dir(traces_root, relative)
                .and_then(|dir| trace_part::load_trace_part(&dir))
                .and_then(TracePartReader::open)
                .is_ok()
        });
        if replacement_durable {
            let dirs = record
                .inputs
                .iter()
                .map(|relative| record_dir(traces_root, relative))
                .collect::<Result<Vec<_>, _>>()?;
            crate::part::remove_part_dirs(&dirs)?;
        } else {
            let mut written_dirs = Vec::new();
            for relative in &record.new {
                let dir = record_dir(traces_root, relative)?;
                if dir.exists() {
                    written_dirs.push(dir);
                }
            }
            crate::part::remove_part_dirs(&written_dirs)?;
        }
        remove_record(&path)?;
    }
    Ok(())
}

async fn restore_missing_bodies(
    registry: &TraceRegistry,
    remote: &RemoteCache,
    inputs: &[Arc<TracePartReader>],
    config: &Config,
) -> Result<(), String> {
    let ids: HashSet<String> = inputs
        .iter()
        .map(|reader| reader.part().meta.id.clone())
        .collect();
    let missing = registry.missing_data_ids(&ids);
    if missing.is_empty() {
        return Ok(());
    }
    match tokio::time::timeout(
        config.max_query_runtime,
        remote
            .storage
            .restore_trace_parts(&remote.trace_parts_root(), &missing),
    )
    .await
    {
        Ok(Ok(())) => {
            remote.record_remote_success();
            Ok(())
        }
        Ok(Err(error)) => {
            remote.record_remote_failure();
            Err(error)
        }
        Err(_) => {
            remote.record_remote_failure();
            Err("trace compaction restore timed out".to_string())
        }
    }
}

fn read_all_spans(inputs: &[Arc<TracePartReader>]) -> Result<Vec<crate::trace::TraceSpan>, String> {
    let mut spans = Vec::new();
    for reader in inputs {
        for segment in &reader.part().meta.tenants {
            spans.extend(reader.query_all_limited(&segment.tenant, usize::MAX, None)?);
        }
    }
    Ok(spans)
}

fn inputs_still_registered(registry: &TraceRegistry, inputs: &[Arc<TracePartReader>]) -> bool {
    let registered: std::collections::HashMap<String, PathBuf> = registry
        .snapshot()
        .into_iter()
        .map(|reader| (reader.part().meta.id.clone(), reader.part().dir.clone()))
        .collect();
    inputs
        .iter()
        .all(|reader| registered.get(&reader.part().meta.id) == Some(&reader.part().dir))
}

/// One compaction pass. Returns whether anything was compacted.
pub async fn compact_once(
    registry: &TraceRegistry,
    deletion_lock: Arc<tokio::sync::RwLock<()>>,
    traces_root: &Path,
    remote: Option<&RemoteCache>,
    config: &Config,
) -> Result<bool, String> {
    let _deletion_guard = deletion_lock.read_owned().await;
    let Some(inputs) = select_inputs(&registry.snapshot()) else {
        return Ok(false);
    };
    if !inputs_still_registered(registry, &inputs) {
        return Ok(false);
    }
    let Some(_memory_charge) = config.memory_account.try_admit(
        inputs
            .iter()
            .map(|reader| stored_bytes(reader))
            .sum::<u64>()
            .saturating_mul(SPAN_DECODE_EXPANSION),
    ) else {
        tracing::debug!("trace compaction waiting for memory account room");
        return Ok(false);
    };
    if let Some(cache) = remote {
        restore_missing_bodies(registry, cache, &inputs, config).await?;
    }

    let arena = crate::memprof::enter(crate::memprof::Arena::Merge);
    let spans = read_all_spans(&inputs)?;
    let span_count = spans.len();
    let new_parts = trace_part::flush_trace_spans(&spans, traces_root, config.row_group_size)
        .map_err(|error| format!("trace compaction failed to write its replacement: {error}"))?;
    drop(spans);
    if new_parts.is_empty() {
        return Ok(false);
    }

    let input_descriptors: Vec<TraceManifestPart> = inputs
        .iter()
        .map(|reader| TraceManifestPart {
            id: reader.part().meta.id.clone(),
            partition: reader.part().meta.partition.clone(),
        })
        .collect();
    let input_dirs: Vec<PathBuf> = inputs
        .iter()
        .map(|reader| reader.part().dir.clone())
        .collect();
    let record = CompactRecord {
        new: new_parts
            .iter()
            .map(|part| relative_dir(traces_root, &part.dir))
            .collect::<Result<_, _>>()?,
        inputs: input_dirs
            .iter()
            .map(|dir| relative_dir(traces_root, dir))
            .collect::<Result<_, _>>()?,
    };
    let record_path = write_record(traces_root, &new_parts[0].meta.id, &record)?;
    drop(arena);

    if let Some(cache) = remote {
        match cache
            .storage
            .replace_trace_parts(&new_parts, &input_descriptors)
            .await
        {
            Ok(_) => cache.record_remote_success(),
            Err(error) if is_inputs_changed_error(&error) => {
                let new_dirs: Vec<PathBuf> =
                    new_parts.iter().map(|part| part.dir.clone()).collect();
                crate::part::remove_part_dirs(&new_dirs)?;
                remove_record(&record_path)?;
                tracing::info!(%error, "trace compaction skipped: inputs changed under it");
                return Ok(false);
            }
            Err(error) => {
                cache.record_remote_failure();
                return Err(error);
            }
        }
    }

    let opened = TraceRegistry::open_parts(new_parts.clone())?;
    let input_ids: Vec<String> = input_descriptors
        .iter()
        .map(|part| part.id.clone())
        .collect();
    {
        let _guard =
            crate::part_registry::PartRegistry::write_without_convoy(registry.operation_lock())
                .await;
        registry.register_opened(opened);
        registry.unregister(&input_ids);
        crate::part::remove_part_dirs(&input_dirs)?;
    }
    remove_record(&record_path)?;

    tracing::info!(
        inputs = inputs.len(),
        outputs = new_parts.len(),
        spans = span_count,
        "trace compaction replaced a tier"
    );
    Ok(true)
}

/// The compaction worker, on the merge cadence like the other two.
pub async fn compact_loop(
    registry: Arc<TraceRegistry>,
    deletion_lock: Arc<tokio::sync::RwLock<()>>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    healthy: Arc<AtomicBool>,
    mut drain_rx: watch::Receiver<bool>,
) {
    let traces_root = config.data_dir.join("traces");
    let mut ticker = interval(config.merge_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = wait_for_drain(&mut drain_rx) => return,
        }
        loop {
            match compact_once(
                &registry,
                deletion_lock.clone(),
                &traces_root,
                remote_cache.as_deref(),
                &config,
            )
            .await
            {
                Ok(true) => {
                    healthy.store(true, Ordering::Release);
                }
                Ok(false) => {
                    healthy.store(true, Ordering::Release);
                    break;
                }
                Err(error) => {
                    healthy.store(false, Ordering::Release);
                    tracing::error!(%error, "trace compaction failed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::test_tenant;
    use crate::trace::{TraceSpan, normalize_request};
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("signy-trace-merge-{tag}-{}", uuid::Uuid::new_v4()))
    }

    fn flush_one_part(root: &Path, part_index: usize) {
        let base_ns = 1_772_000_000_000_000_000u64 + part_index as u64 * 1_000_000;
        let spans = (0..3u64)
            .map(|span_index| Span {
                trace_id: vec![part_index as u8 + 1; 16],
                span_id: vec![span_index as u8 + 1; 8],
                start_time_unix_nano: base_ns + span_index,
                end_time_unix_nano: base_ns + span_index + 1_000,
                ..Default::default()
            })
            .collect();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let spans: Vec<TraceSpan> = normalize_request(&test_tenant(), request).unwrap();
        trace_part::flush_trace_spans(&spans, root, 16).unwrap();
    }

    fn all_spans(registry: &TraceRegistry) -> Vec<(String, String, i64)> {
        let mut spans: Vec<(String, String, i64)> = registry
            .snapshot()
            .iter()
            .flat_map(|reader| reader.query_all(&test_tenant()).unwrap())
            .map(|span| (span.trace_id, span.span_id, span.start_time_ns))
            .collect();
        spans.sort();
        spans
    }

    fn test_config(root: &Path) -> Config {
        Config {
            data_dir: root.parent().unwrap().to_path_buf(),
            row_group_size: 16,
            ..Config::default()
        }
    }

    #[test]
    fn the_trigger_needs_a_full_tier_in_one_partition() {
        let root = temp_root("trigger");
        for part_index in 0..COMPACT_MIN_PARTS - 1 {
            flush_one_part(&root, part_index);
        }
        let registry =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        assert!(select_inputs(&registry.snapshot()).is_none());

        flush_one_part(&root, COMPACT_MIN_PARTS);
        let registry =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        assert_eq!(
            select_inputs(&registry.snapshot()).map(|inputs| inputs.len()),
            Some(COMPACT_MIN_PARTS)
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_compaction_replaces_the_tier_and_answers_do_not_change() {
        let root = temp_root("equivalence");
        for part_index in 0..COMPACT_MIN_PARTS {
            flush_one_part(&root, part_index);
        }
        let registry =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        let before = all_spans(&registry);
        let config = test_config(&root);

        let compacted = compact_once(
            &registry,
            Arc::new(tokio::sync::RwLock::new(())),
            &root,
            None,
            &config,
        )
        .await
        .unwrap();

        assert!(compacted);
        assert_eq!(registry.snapshot().len(), 1);
        assert_eq!(
            all_spans(&registry),
            before,
            "a compaction changes nothing a query reads"
        );
        assert!(read_records(&root).unwrap().is_empty());
        assert_eq!(trace_part::discover_trace_parts(&root).unwrap().len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    fn descriptor_of(reader: &TracePartReader) -> TraceManifestPart {
        TraceManifestPart {
            id: reader.part().meta.id.clone(),
            partition: reader.part().meta.partition.clone(),
        }
    }

    #[tokio::test]
    async fn a_remote_compaction_swaps_the_manifest_in_one_commit_and_repeats_as_a_no_op() {
        let data_dir = temp_root("remote");
        let traces_root = data_dir.join("traces");
        for part_index in 0..COMPACT_MIN_PARTS {
            flush_one_part(&traces_root, part_index);
        }
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        storage
            .publish_trace_parts(&trace_part::discover_trace_parts(&traces_root).unwrap())
            .await
            .unwrap();
        let cache = RemoteCache::new(storage.clone(), data_dir.join("parts"));
        let registry =
            TraceRegistry::load_from_disk(&traces_root, Arc::new(tokio::sync::RwLock::new(())))
                .unwrap();
        let before = all_spans(&registry);
        let input_descriptors: Vec<TraceManifestPart> = registry
            .snapshot()
            .iter()
            .map(|reader| descriptor_of(reader))
            .collect();
        let generation_before = storage.load_trace_manifest().await.unwrap().generation;
        let config = Config {
            data_dir: data_dir.clone(),
            row_group_size: 16,
            ..Config::default()
        };

        assert!(
            compact_once(
                &registry,
                Arc::new(tokio::sync::RwLock::new(())),
                &traces_root,
                Some(&cache),
                &config,
            )
            .await
            .unwrap()
        );

        let manifest = storage.load_trace_manifest().await.unwrap();
        assert_eq!(manifest.generation, generation_before + 1);
        let replacement = registry.snapshot()[0].part().clone();
        assert_eq!(manifest.parts, vec![descriptor_of(&registry.snapshot()[0])]);
        assert_eq!(all_spans(&registry), before);

        let repeated = storage
            .replace_trace_parts(&[replacement], &input_descriptors)
            .await
            .unwrap();
        assert_eq!(repeated.generation, manifest.generation);
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[tokio::test]
    async fn a_remote_restart_replays_a_compaction_that_crashed_before_the_manifest_swap() {
        let data_dir = temp_root("remote-replay");
        let traces_root = data_dir.join("traces");
        for part_index in 0..COMPACT_MIN_PARTS {
            flush_one_part(&traces_root, part_index);
        }
        let storage = crate::object_storage::ObjectStorage::in_memory();
        storage
            .publish_trace_parts(&trace_part::discover_trace_parts(&traces_root).unwrap())
            .await
            .unwrap();
        let registry =
            TraceRegistry::load_from_disk(&traces_root, Arc::new(tokio::sync::RwLock::new(())))
                .unwrap();
        let before = all_spans(&registry);
        let inputs = select_inputs(&registry.snapshot()).unwrap();
        let spans = read_all_spans(&inputs).unwrap();
        let new_parts = trace_part::flush_trace_spans(&spans, &traces_root, 16).unwrap();
        let record = CompactRecord {
            new: new_parts
                .iter()
                .map(|part| relative_dir(&traces_root, &part.dir).unwrap())
                .collect(),
            inputs: inputs
                .iter()
                .map(|reader| relative_dir(&traces_root, &reader.part().dir).unwrap())
                .collect(),
        };
        write_record(&traces_root, &new_parts[0].meta.id, &record).unwrap();

        let manifest = storage
            .reconcile_trace_local_cache(&traces_root)
            .await
            .unwrap();

        assert_eq!(
            manifest
                .parts
                .iter()
                .map(|part| part.id.clone())
                .collect::<Vec<_>>(),
            vec![new_parts[0].meta.id.clone()],
            "the replacement stands alone in the manifest, not beside its inputs"
        );
        assert!(read_records(&traces_root).unwrap().is_empty());
        let restarted = TraceRegistry::load_from_manifest(
            &traces_root,
            &manifest,
            Arc::new(tokio::sync::RwLock::new(())),
        )
        .unwrap();
        assert_eq!(all_spans(&restarted), before);
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[test]
    fn recovery_keeps_one_copy_when_the_replacement_is_durable() {
        let root = temp_root("recover-durable");
        for part_index in 0..COMPACT_MIN_PARTS {
            flush_one_part(&root, part_index);
        }
        let registry =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        let before = all_spans(&registry);
        let inputs = select_inputs(&registry.snapshot()).unwrap();
        let spans = read_all_spans(&inputs).unwrap();
        let new_parts = trace_part::flush_trace_spans(&spans, &root, 16).unwrap();
        let record = CompactRecord {
            new: new_parts
                .iter()
                .map(|part| relative_dir(&root, &part.dir).unwrap())
                .collect(),
            inputs: inputs
                .iter()
                .map(|reader| relative_dir(&root, &reader.part().dir).unwrap())
                .collect(),
        };
        write_record(&root, &new_parts[0].meta.id, &record).unwrap();

        recover_local_compactions(&root).unwrap();

        let recovered =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        assert_eq!(all_spans(&recovered), before);
        assert!(read_records(&root).unwrap().is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn recovery_keeps_the_inputs_when_the_replacement_never_became_durable() {
        let root = temp_root("recover-undone");
        flush_one_part(&root, 0);
        let registry =
            TraceRegistry::load_from_disk(&root, Arc::new(tokio::sync::RwLock::new(()))).unwrap();
        let input_dir = registry.snapshot()[0].part().dir.clone();
        let record = CompactRecord {
            new: vec!["2026-02-25/never-written".to_string()],
            inputs: vec![relative_dir(&root, &input_dir).unwrap()],
        };
        write_record(&root, "never-written", &record).unwrap();

        recover_local_compactions(&root).unwrap();

        assert!(input_dir.exists(), "the inputs survive an unfinished pass");
        assert!(read_records(&root).unwrap().is_empty());
        std::fs::remove_dir_all(&root).ok();
    }
}
