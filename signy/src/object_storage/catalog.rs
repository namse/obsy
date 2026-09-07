/// Immutable part objects plus a compare-and-swap manifest.
///
/// `prefix` is the path component of SIGNY_OBJECT_STORE_URL. Credentials
/// and endpoint settings are consumed by object_store from the process
/// environment (AWS_*, OBJECT_STORE_* and backend-specific keys).
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    /// Requests issued through `store`, counted by kind. See
    /// [`counting_store`] for why the count and not the amount.
    ops: Arc<ObjectStoreOps>,
    prefix: ObjectPath,
    manifest_update: tokio::sync::Mutex<()>,
    /// The local filesystem backend does not implement conditional updates.
    /// It is only exposed as a single-process development backend, where the
    /// process-local mutex plus LocalFileSystem's staged rename gives us an
    /// atomic replacement.
    local_manifest_overwrite: bool,
    /// Told once, when the epoch check first fires.
    ///
    /// Held here rather than returned to each caller so that every writer —
    /// flush, merge, retention, the final force-flush — reacts identically
    /// without any of them having to know what fencing is. They all see an
    /// ordinary manifest error; the drain has already begun by then.
    fence_sink: std::sync::OnceLock<Arc<crate::shutdown::ShutdownState>>,
    /// This instance's claim on the prefix, or 0 while unclaimed.
    ///
    /// The architecture assumes one writer; nothing used to enforce it, so two
    /// processes on the same prefix each believed they owned it. The manifest
    /// CAS stops a lost update but not two divergent local WALs, and not one
    /// instance's retention expiring a part the other has just registered.
    writer_epoch: AtomicU64,
    /// Catalog files checksummed while restoring. The expensive part of a
    /// startup at scale is this, not the store round trips: every part's bloom,
    /// stream index and metadata are read and verified from local disk. Counted
    /// so a test can catch a redundant pass without running at the scale where
    /// the seconds show.
    #[cfg(test)]
    catalog_validations: AtomicU64,
}

/// Coordinates all mutations of the local object-store cache. Readers hold a
/// shared guard while their Parquet files are open; restore and eviction hold
/// an exclusive guard.
pub struct RemoteCache {
    pub storage: Arc<ObjectStorage>,
    pub parts_root: PathBuf,
    /// Object-store failures since the last success.
    ///
    /// This was a health bit plus a failure generation, where a success cleared
    /// the bit only if its operation had started in the current generation. The
    /// guard was aimed at a real hazard — a slow success must not clear a newer
    /// failure — but it made a *single* failed request mean "the store is
    /// down", and that is what `/ready` reads.
    ///
    /// Measured at a 3% injected write-error rate, which the engine survives
    /// with no ingest errors and no lost data: `remote_healthy` flipped 14-17
    /// times a minute and read false 34-59% of the time. An orchestrator
    /// watching that pulls the instance in and out of service over an error
    /// rate that cost nothing.
    remote_failures: Arc<AtomicU32>,
    cache_healthy: Arc<AtomicBool>,
}

/// Consecutive failures that constitute an outage rather than a bad request.
///
/// A store that is genuinely unreachable fails everything, so it crosses this
/// in the time of a few operations. An isolated failure between successes never
/// does.
///
/// What is given up is the old generation guard: a slow success that started
/// before an outage now resets the count, delaying detection by one more round
/// of failures. That is a bounded delay rather than the indefinite masking the
/// guard was written to prevent, because during a real outage failures vastly
/// outnumber stale successes.
const REMOTE_FAILURE_THRESHOLD: u32 = 3;

impl RemoteCache {
    pub fn new(storage: Arc<ObjectStorage>, parts_root: PathBuf) -> Self {
        Self {
            storage,
            parts_root,
            remote_failures: Arc::new(AtomicU32::new(0)),
            cache_healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.is_remote_healthy() && self.is_cache_healthy()
    }

    pub fn is_remote_healthy(&self) -> bool {
        self.consecutive_remote_failures() < REMOTE_FAILURE_THRESHOLD
    }

    /// Failure pressure below the threshold, which the health flag hides by
    /// design. An operator watching this sees a store degrading before it is
    /// declared down.
    pub fn consecutive_remote_failures(&self) -> u32 {
        self.remote_failures.load(Ordering::Acquire)
    }

    pub fn is_cache_healthy(&self) -> bool {
        self.cache_healthy.load(Ordering::Acquire)
    }

    /// The store answered. Callers report the outcome of the operation they
    /// just finished; nothing has to be captured beforehand.
    pub fn record_remote_success(&self) {
        self.remote_failures.store(0, Ordering::Release);
    }

    pub fn record_remote_failure(&self) {
        let _ = self
            .remote_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |failures| {
                Some(failures.saturating_add(1))
            });
    }

    pub fn mark_cache_healthy(&self) {
        self.cache_healthy.store(true, Ordering::Release);
    }

    pub fn mark_cache_unhealthy(&self) {
        self.cache_healthy.store(false, Ordering::Release);
    }

    pub fn trace_parts_root(&self) -> PathBuf {
        self.parts_root
            .parent()
            .map(|parent| parent.join("traces"))
            .unwrap_or_else(|| PathBuf::from("traces"))
    }

    pub fn metric_parts_root(&self) -> PathBuf {
        self.parts_root
            .parent()
            .map(|parent| parent.join("metrics"))
            .unwrap_or_else(|| PathBuf::from("metrics"))
    }

    /// Drive the remote to unhealthy in one call, for tests that need the
    /// state rather than the path into it.
    #[cfg(test)]
    pub fn mark_unhealthy(&self) {
        for _ in 0..REMOTE_FAILURE_THRESHOLD {
            self.record_remote_failure();
        }
    }
}

impl ObjectStorage {
    /// See `catalog_validations`.
    #[cfg(test)]
    pub fn catalog_validations(&self) -> u64 {
        self.catalog_validations.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn record_catalog_validation(&self) {
        self.catalog_validations.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(not(test))]
    fn record_catalog_validation(&self) {}


    pub fn from_url(url: &str) -> Result<Self, String> {
        let url =
            url::Url::parse(url).map_err(|error| format!("invalid object-store URL: {error}"))?;
        let local_manifest_overwrite = url.scheme() == "file";
        if local_manifest_overwrite {
            tracing::warn!(
                %url,
                "object store is a local filesystem path: manifest updates use overwrite instead \
of compare-and-swap and are only safe for a single process on a local disk. Do not use this for \
production or on shared/network storage."
            );
        }
        let options = normalized_object_store_options(std::env::vars());
        let (store, prefix) = object_store::parse_url_opts(&url, options)
            .map_err(|error| format!("failed to configure object store: {error}"))?;
        // Tier B load gate: wrap the real store with seeded latency/fault
        // injection only when the load knobs are present. Absent the knobs this
        // is a no-op and the object-store construction path is unchanged.
        let store: Arc<dyn ObjectStore> = match fault_store::FaultConfig::from_env()? {
            Some(config) => Arc::new(fault_store::LatencyFaultStore::new(Arc::from(store), config)),
            None => Arc::from(store),
        };
        Ok(Self::wrapping(store, prefix, local_manifest_overwrite))
    }

    /// The one place the store is installed, so the operation counter cannot be
    /// left off a backend by adding a constructor that forgets it.
    fn wrapping(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        local_manifest_overwrite: bool,
    ) -> Self {
        let ops = Arc::new(ObjectStoreOps::default());
        Self {
            store: Arc::new(CountingStore::new(store, ops.clone())),
            ops,
            prefix,
            manifest_update: tokio::sync::Mutex::new(()),
            local_manifest_overwrite,
            fence_sink: std::sync::OnceLock::new(),
            writer_epoch: AtomicU64::new(0),
            #[cfg(test)]
            catalog_validations: AtomicU64::new(0),
        }
    }

    /// Object-store requests this process has issued, by kind.
    pub fn operation_counts(&self) -> ObjectStoreOpCounts {
        self.ops.snapshot()
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self::wrapping(
            Arc::new(object_store::memory::InMemory::new()),
            ObjectPath::from("signy-test"),
            false,
        )
    }

    /// Two handles over one backing store: what two processes pointed at the
    /// same prefix actually look like.
    #[cfg(test)]
    pub fn sharing_store_for_test(store: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self::wrapping(
            store,
            ObjectPath::from("signy-test"),
            false,
        ))
    }

    /// An in-memory store whose every write fails, for the paths that must
    /// report a failure rather than apply a change that is not durable.
    #[cfg(test)]
    pub fn in_memory_with_failing_writes() -> Self {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Self::wrapping(
            Arc::new(fault_store::LatencyFaultStore::new(
                inner,
                fault_store::FaultConfig::for_test(0, 0, 0, 1.0, 1),
            )),
            ObjectPath::from("signy-test"),
            false,
        )
    }

    /// Wrap an arbitrary object store, used by tests to inject fault-injecting
    /// backends. The store is treated as a full conditional-put backend (no
    /// local overwrite shortcut), exercising the real CAS manifest path.
    #[cfg(test)]
    pub fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self::wrapping(store, ObjectPath::from(prefix), false)
    }

    fn path(&self, relative: &str) -> ObjectPath {
        if self.prefix.as_ref().is_empty() {
            ObjectPath::from(relative)
        } else {
            ObjectPath::from(format!("{}/{relative}", self.prefix.as_ref()))
        }
    }

    fn manifest_path(&self) -> ObjectPath {
        self.path(MANIFEST_FILE)
    }

    fn trace_manifest_path(&self) -> ObjectPath {
        self.path(TRACE_MANIFEST_FILE)
    }

    fn part_path(&self, part: &ManifestPart, file: &str) -> ObjectPath {
        self.path(&format!("parts/{}/{}/{}", part.partition, part.id, file))
    }

    fn tenant_policy_path(&self, tenant: &str) -> ObjectPath {
        self.path(&format!("{TENANT_POLICY_PREFIX}/{tenant}.json"))
    }

    fn trace_part_path(&self, part: &TraceManifestPart, file: &str) -> ObjectPath {
        self.path(&format!(
            "trace_parts/{}/{}/{}",
            part.partition, part.id, file
        ))
    }

    fn metric_manifest_path(&self) -> ObjectPath {
        self.path(METRIC_MANIFEST_FILE)
    }

    fn metric_part_path(&self, part: &MetricManifestPart, file: &str) -> ObjectPath {
        self.path(&format!(
            "metric_parts/{}/{}/{}",
            part.partition, part.id, file
        ))
    }

    async fn load_manifest_versioned(&self) -> Result<LoadedManifest, String> {
        let path = self.manifest_path();
        match self.store.get(&path).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| format!("failed to read manifest body: {error}"))?;
                let manifest: Manifest = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid object-store manifest: {error}"))?;
                validate_manifest(&manifest)?;
                Ok(LoadedManifest { manifest, version })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(LoadedManifest {
                manifest: Manifest::default(),
                version: None,
            }),
            Err(error) => Err(format!("failed to load object-store manifest: {error}")),
        }
    }

    pub async fn load_manifest(&self) -> Result<Manifest, String> {
        Ok(self.load_manifest_versioned().await?.manifest)
    }

    /// Refuse to start when the configured store does not actually enforce
    /// conditional writes.
    ///
    /// This does not test `object_store` — whether `AmazonS3` implements
    /// `If-Match` correctly is that crate's problem and its test suite's. What
    /// it tests is **our configuration**: `from_url` hands the environment
    /// straight to `object_store`, and nothing else checks that the store it
    /// built back actually does compare-and-swap. Get
    /// `OBJECT_STORE_CONDITIONAL_PUT` wrong and every manifest guarantee in
    /// this engine — lost-update protection, merge input revalidation, writer
    /// fencing — silently rests on nothing.
    ///
    /// The check is the *negative* path. A positive one proves nothing: the
    /// first manifest write of a fresh prefix succeeds whether or not the
    /// condition was honoured. What must hold is that a write which should be
    /// rejected **is** rejected.
    pub async fn verify_conditional_put(&self) -> Result<(), String> {
        if self.local_manifest_overwrite {
            // `file://` is a declared single-process development backend that
            // deliberately opts out of CAS, and `from_url` already warns.
            return Ok(());
        }
        let probe = self.path("_preflight/conditional-put-probe");
        // Any leftover from an aborted earlier boot would make the first
        // create fail for the wrong reason.
        let _ = self.store.delete(&probe).await;

        let created = self
            .store
            .put_opts(
                &probe,
                Bytes::from_static(b"signy conditional-put probe").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| {
                format!("conditional-put preflight could not write its probe object: {error}")
            })?;

        let outcome = self.probe_rejections(&probe, created).await;
        // Best effort: a leftover probe only costs the next boot one delete.
        let _ = self.store.delete(&probe).await;
        outcome
    }

    async fn probe_rejections(&self, probe: &ObjectPath, created: PutResult) -> Result<(), String> {
        let recreated = self
            .store
            .put_opts(
                probe,
                Bytes::from_static(b"second create").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        if recreated.is_ok() {
            return Err(Self::preflight_failure(
                "a second PutMode::Create over an existing object succeeded",
            ));
        }

        // A version that was valid and no longer is: exactly the shape of the
        // lost update the manifest CAS exists to prevent.
        let stale = UpdateVersion {
            e_tag: created.e_tag.clone(),
            version: created.version.clone(),
        };
        if stale.e_tag.is_none() && stale.version.is_none() {
            return Err(Self::preflight_failure(
                "the store returned neither an ETag nor a version, so no write can ever be conditioned on one",
            ));
        }
        self.store
            .put_opts(
                probe,
                Bytes::from_static(b"overwrite").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("conditional-put preflight could not overwrite: {error}"))?;
        let updated = self
            .store
            .put_opts(
                probe,
                Bytes::from_static(b"stale update").into(),
                PutOptions {
                    mode: PutMode::Update(stale),
                    ..Default::default()
                },
            )
            .await;
        if updated.is_ok() {
            return Err(Self::preflight_failure(
                "a PutMode::Update carrying a superseded version succeeded",
            ));
        }
        Ok(())
    }

    fn preflight_failure(what_happened: &str) -> String {
        format!(
            "the configured object store does not enforce conditional writes: {what_happened}. \
Every manifest guarantee in this engine depends on compare-and-swap, so refusing to start is the \
only safe response. For S3-compatible stores set OBJECT_STORE_CONDITIONAL_PUT=etag; for a local \
single-process development store use a file:// URL, which opts out of CAS deliberately."
        )
    }

    /// Where a detected fence is reported. Set once, during startup.
    pub fn set_fence_sink(&self, shutdown: Arc<crate::shutdown::ShutdownState>) {
        let _ = self.fence_sink.set(shutdown);
    }

    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch.load(Ordering::Acquire)
    }

    /// Take ownership of the prefix, and report the epoch taken.
    ///
    /// Called once at startup, before any worker runs. Both manifests carry
    /// the same number so that whichever one a later write touches first
    /// notices a takeover.
    pub async fn claim_writer_epoch(&self) -> Result<u64, String> {
        let mut claimed = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self.load_manifest_versioned().await?;
            let epoch = loaded
                .manifest
                .writer_epoch
                .checked_add(1)
                .ok_or_else(|| "writer epoch overflow".to_string())?;
            let mut next = loaded.manifest.clone();
            next.writer_epoch = epoch;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or_else(|| "manifest generation overflow".to_string())?;
            if self
                .try_put_manifest(&next, loaded.version, self.manifest_path())
                .await?
            {
                claimed = Some(epoch);
                break;
            }
        }
        let Some(epoch) = claimed else {
            return Err("writer epoch claim CAS retry limit exceeded".to_string());
        };

        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_trace_manifest_versioned().await?;
            let mut next = loaded.clone();
            next.writer_epoch = epoch;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or_else(|| "trace manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode trace manifest: {error}"))?;
            let mode = self.put_mode(version);
            match self
                .store
                .put_opts(
                    &self.trace_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    self.claim_metric_writer_epoch(epoch).await?;
                    // Published only once all three manifests agree: a
                    // half-claimed prefix would fence this instance off its
                    // own trace or metric writes.
                    self.writer_epoch.store(epoch, Ordering::Release);
                    tracing::info!(epoch, "claimed the object-store writer epoch");
                    return Ok(epoch);
                }
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to claim the trace manifest: {error}")),
            }
        }
        Err("trace writer epoch claim CAS retry limit exceeded".to_string())
    }

    /// The metric half of the claim, run after the other two manifests carry
    /// the epoch. Split from `claim_writer_epoch` only so the publish order in
    /// that function stays readable; a caller always runs both.
    async fn claim_metric_writer_epoch(&self, epoch: u64) -> Result<(), String> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_metric_manifest_versioned().await?;
            let mut next = loaded.clone();
            next.writer_epoch = epoch;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or_else(|| "metric manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode metric manifest: {error}"))?;
            let mode = self.put_mode(version);
            match self
                .store
                .put_opts(
                    &self.metric_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to claim the metric manifest: {error}")),
            }
        }
        Err("metric writer epoch claim CAS retry limit exceeded".to_string())
    }

    fn put_mode(&self, version: Option<UpdateVersion>) -> PutMode {
        match version {
            Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
            Some(version) => PutMode::Update(version),
            None => PutMode::Create,
        }
    }

    async fn try_put_manifest(
        &self,
        manifest: &Manifest,
        version: Option<UpdateVersion>,
        path: ObjectPath,
    ) -> Result<bool, String> {
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|error| format!("failed to encode manifest: {error}"))?;
        let mode = self.put_mode(version);
        match self
            .store
            .put_opts(
                &path,
                body.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(error) => Err(format!("failed to update manifest: {error}")),
        }
    }

    /// Refuse to write when the loaded manifest is owned by someone else.
    ///
    /// Checked on every CAS rather than periodically, so the fence lands on
    /// the first write after a takeover instead of at the next poll.
    fn check_epoch(&self, observed: u64) -> Result<(), String> {
        let held = self.writer_epoch();
        if held == 0 || observed == held {
            return Ok(());
        }
        if let Some(shutdown) = self.fence_sink.get() {
            shutdown.mark_fenced();
        }
        Err(format!(
            "{FENCED_ERROR}: this instance holds writer epoch {held} but the manifest now carries \
{observed}"
        ))
    }

    async fn load_trace_manifest_versioned(
        &self,
    ) -> Result<(TraceManifest, Option<UpdateVersion>), String> {
        match self.store.get(&self.trace_manifest_path()).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| format!("failed to read trace manifest body: {error}"))?;
                let manifest: TraceManifest = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid trace object-store manifest: {error}"))?;
                validate_trace_manifest(&manifest)?;
                Ok((manifest, version))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((TraceManifest::default(), None)),
            Err(error) => Err(format!(
                "failed to load trace object-store manifest: {error}"
            )),
        }
    }

    pub async fn load_trace_manifest(&self) -> Result<TraceManifest, String> {
        Ok(self.load_trace_manifest_versioned().await?.0)
    }

    pub async fn publish_trace_parts(&self, added: &[TracePart]) -> Result<TraceManifest, String> {
        for part in added {
            let id = part.meta.id.clone();
            TracePartReader::open(part.clone())
                .map_err(|error| format!("refusing to publish invalid trace part {id}: {error}"))?;
        }
        for part in added {
            self.upload_trace_part(part).await?;
        }

        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_trace_manifest_versioned().await?;
            self.check_epoch(loaded.writer_epoch)?;
            let mut next = loaded.clone();
            for part in added {
                let descriptor = TraceManifestPart {
                    id: part.meta.id.clone(),
                    partition: part.meta.partition.clone(),
                };
                if let Some(existing) = next.parts.iter().find(|item| item.id == descriptor.id) {
                    if existing != &descriptor {
                        return Err(format!(
                            "trace manifest part ID collision: {}",
                            descriptor.id
                        ));
                    }
                } else {
                    next.parts.push(descriptor);
                }
            }
            next.parts.sort_by(|left, right| {
                (&left.partition, &left.id).cmp(&(&right.partition, &right.id))
            });
            if next.parts == loaded.parts {
                return Ok(loaded);
            }
            next.generation = loaded
                .generation
                .checked_add(1)
                .ok_or_else(|| "trace manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode trace manifest: {error}"))?;
            let mode = match version {
                Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            };
            match self
                .store
                .put_opts(
                    &self.trace_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update trace manifest: {error}")),
            }
        }
        Err("trace manifest compare-and-swap retry limit exceeded".to_string())
    }

    /// Removes trace descriptors from the manifest using the same CAS
    /// semantics as publication. The immutable objects are deleted only after
    /// the manifest no longer exposes them.
    pub async fn remove_trace_parts(
        &self,
        removed: &[TraceManifestPart],
    ) -> Result<TraceManifest, String> {
        if removed.is_empty() {
            return self.load_trace_manifest().await;
        }
        let removed_ids: HashSet<&str> = removed.iter().map(|part| part.id.as_str()).collect();
        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_trace_manifest_versioned().await?;
            self.check_epoch(loaded.writer_epoch)?;
            let present = loaded
                .parts
                .iter()
                .filter(|part| removed_ids.contains(part.id.as_str()))
                .count();
            // Removal is the only thing this writes, so it is idempotent per
            // id: a batch that mixes ids an earlier tick already removed with
            // ids that have only just expired removes what is left rather than
            // failing, which is what keeps a retry from wedging forever.
            if present == 0 {
                return Ok(loaded);
            }
            let mut next = loaded.clone();
            next.parts
                .retain(|part| !removed_ids.contains(part.id.as_str()));
            next.generation = loaded
                .generation
                .checked_add(1)
                .ok_or_else(|| "trace manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode trace manifest: {error}"))?;
            let mode = match version {
                Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            };
            match self
                .store
                .put_opts(
                    &self.trace_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update trace manifest: {error}")),
            }
        }
        Err("trace manifest retention CAS retry limit exceeded".to_string())
    }

    async fn load_metric_manifest_versioned(
        &self,
    ) -> Result<(MetricManifest, Option<UpdateVersion>), String> {
        match self.store.get(&self.metric_manifest_path()).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| format!("failed to read metric manifest body: {error}"))?;
                let manifest: MetricManifest = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid metric object-store manifest: {error}"))?;
                validate_metric_manifest(&manifest)?;
                Ok((manifest, version))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((MetricManifest::default(), None)),
            Err(error) => Err(format!(
                "failed to load metric object-store manifest: {error}"
            )),
        }
    }

    pub async fn load_metric_manifest(&self) -> Result<MetricManifest, String> {
        Ok(self.load_metric_manifest_versioned().await?.0)
    }

    /// Uploads immutable metric part files, then atomically adds and removes
    /// their descriptors in one manifest CAS. Modeled on the log `publish`
    /// rather than the trace one because the metric compactor replaces its
    /// inputs, and a replacement whose add and remove land in two generations
    /// has a window where both the inputs and the output answer queries.
    pub async fn publish_metric_parts(
        &self,
        added: &[SeriesPart],
        removed: &[MetricManifestPart],
    ) -> Result<MetricManifest, String> {
        for part in added {
            let id = part.meta.id.clone();
            SeriesPartReader::open(part.clone()).map_err(|error| {
                format!("refusing to publish invalid metric part {id}: {error}")
            })?;
        }
        for part in added {
            self.upload_metric_part(part).await?;
        }

        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_metric_manifest_versioned().await?;
            self.check_epoch(loaded.writer_epoch)?;
            let removed_ids: HashSet<&str> = removed.iter().map(|part| part.id.as_str()).collect();
            // The same replacement discipline as the log publish: a retry that
            // observes its inputs already gone accepts only the exact
            // idempotent end state, and a compaction whose inputs another
            // writer touched is refused rather than reapplied.
            if !removed_ids.is_empty() {
                let present_removed = loaded
                    .parts
                    .iter()
                    .filter(|part| removed_ids.contains(part.id.as_str()))
                    .count();
                let all_added_present = added.iter().all(|part| {
                    let descriptor = MetricManifestPart::from(part);
                    loaded.parts.iter().any(|existing| existing == &descriptor)
                });
                if present_removed == 0 && all_added_present {
                    return Ok(loaded);
                }
                if !added.is_empty() && present_removed != removed_ids.len() {
                    return Err(format!(
                        "{INPUTS_CHANGED_ERROR}: expected {} input metric parts, found {present_removed}",
                        removed_ids.len()
                    ));
                }
            }
            let mut next = loaded.clone();
            next.parts
                .retain(|part| !removed_ids.contains(part.id.as_str()));
            for part in added.iter().map(MetricManifestPart::from) {
                if let Some(existing) = next.parts.iter().find(|item| item.id == part.id) {
                    if existing != &part {
                        return Err(format!("metric manifest part ID collision: {}", part.id));
                    }
                } else {
                    next.parts.push(part);
                }
            }
            next.parts.sort_by(|left, right| {
                (&left.partition, &left.id).cmp(&(&right.partition, &right.id))
            });
            if next.parts == loaded.parts {
                return Ok(loaded);
            }
            next.generation = loaded
                .generation
                .checked_add(1)
                .ok_or_else(|| "metric manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode metric manifest: {error}"))?;
            let mode = self.put_mode(version);
            match self
                .store
                .put_opts(
                    &self.metric_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update metric manifest: {error}")),
            }
        }
        Err("metric manifest compare-and-swap retry limit exceeded".to_string())
    }

    /// Removal alone, idempotent per id for the same reason the trace removal
    /// is: retention retries must remove what is left rather than wedge.
    pub async fn remove_metric_parts(
        &self,
        removed: &[MetricManifestPart],
    ) -> Result<MetricManifest, String> {
        if removed.is_empty() {
            return self.load_metric_manifest().await;
        }
        self.publish_metric_parts(&[], removed).await
    }

    pub async fn delete_metric_part_objects(
        &self,
        parts: &[MetricManifestPart],
    ) -> Result<(), String> {
        for part in parts {
            for file in METRIC_PART_FILES {
                match self.store.delete(&self.metric_part_path(part, file)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to delete remote metric part {}/{} file {file}: {error}",
                            part.partition, part.id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn delete_part_objects(&self, parts: &[ManifestPart]) -> Result<(), String> {
        for part in parts {
            for file in PART_FILES {
                match self.store.delete(&self.part_path(part, file)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to delete remote part {}/{} file {file}: {error}",
                            part.partition, part.id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn delete_trace_part_objects(
        &self,
        parts: &[TraceManifestPart],
    ) -> Result<(), String> {
        for part in parts {
            for file in TRACE_PART_FILES {
                match self.store.delete(&self.trace_part_path(part, file)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to delete remote trace part {}/{} file {file}: {error}",
                            part.partition, part.id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// One tenant's retention policy, written blind.
    ///
    /// One object per tenant is what lets a push be a single unconditional
    /// write: no read-modify-write, no CAS, and no contention between two
    /// tenants pushed concurrently. Ordering between two pushes for the *same*
    /// tenant is the caller's problem, and `TenantPolicy` serializes them.
    pub async fn put_tenant_policy(&self, tenant: &str, body: Vec<u8>) -> Result<(), String> {
        self.store
            .put(&self.tenant_policy_path(tenant), body.into())
            .await
            .map(|_| ())
            .map_err(|error| format!("failed to store the policy for tenant {tenant}: {error}"))
    }

    pub async fn delete_tenant_policy(&self, tenant: &str) -> Result<(), String> {
        match self.store.delete(&self.tenant_policy_path(tenant)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(format!(
                "failed to delete the policy for tenant {tenant}: {error}"
            )),
        }
    }

    /// Every stored policy, as `(file name, body)`. Read once at startup; a
    /// failure here is fatal, so it never returns a partial listing.
    pub async fn load_tenant_policies(&self) -> Result<Vec<(String, Vec<u8>)>, String> {
        use futures_util::StreamExt;

        let prefix = self.path(TENANT_POLICY_PREFIX);
        let mut locations = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta =
                item.map_err(|error| format!("failed to list the tenant policies: {error}"))?;
            locations.push(meta.location);
        }
        let mut policies = Vec::new();
        for location in locations {
            let name = location
                .filename()
                .ok_or_else(|| format!("tenant policy object {location} has no file name"))?
                .to_string();
            let bytes = self
                .store
                .get(&location)
                .await
                .map_err(|error| format!("failed to read tenant policy {location}: {error}"))?
                .bytes()
                .await
                .map_err(|error| format!("failed to read tenant policy {location}: {error}"))?;
            policies.push((name, bytes.to_vec()));
        }
        Ok(policies)
    }

    fn delete_request_path(&self, tenant: &str, request_id: &str) -> ObjectPath {
        self.path(&format!("{DELETE_REQUEST_PREFIX}/{tenant}/{request_id}.json"))
    }

    /// One object per request, for the same reason as one object per policy: a
    /// submission is a single unconditional write with nothing to contend on.
    pub async fn put_delete_request(
        &self,
        tenant: &str,
        request_id: &str,
        body: Vec<u8>,
    ) -> Result<(), String> {
        self.store
            .put(&self.delete_request_path(tenant, request_id), body.into())
            .await
            .map(|_| ())
            .map_err(|error| {
                format!("failed to store delete request {request_id} for tenant {tenant}: {error}")
            })
    }

    pub async fn remove_delete_request(
        &self,
        tenant: &str,
        request_id: &str,
    ) -> Result<(), String> {
        match self
            .store
            .delete(&self.delete_request_path(tenant, request_id))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(format!(
                "failed to remove delete request {request_id} for tenant {tenant}: {error}"
            )),
        }
    }

    /// Every stored request body. Read once at startup; a failure here is fatal,
    /// because starting with a subset would serve data a tenant asked to have
    /// deleted.
    pub async fn load_delete_requests(&self) -> Result<Vec<Vec<u8>>, String> {
        use futures_util::StreamExt;

        let prefix = self.path(DELETE_REQUEST_PREFIX);
        let mut locations = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta =
                item.map_err(|error| format!("failed to list the delete requests: {error}"))?;
            locations.push(meta.location);
        }
        let mut requests = Vec::new();
        for location in locations {
            let bytes = self
                .store
                .get(&location)
                .await
                .map_err(|error| format!("failed to read delete request {location}: {error}"))?
                .bytes()
                .await
                .map_err(|error| format!("failed to read delete request {location}: {error}"))?;
            requests.push(bytes.to_vec());
        }
        Ok(requests)
    }

    /// Deletes immutable objects that are absent from both manifests only
    /// after a grace period. Retention removes manifest visibility first; this
    /// pass is the crash-safe, delayed physical garbage collector.
    pub async fn garbage_collect_orphans(
        &self,
        grace_period: std::time::Duration,
    ) -> Result<usize, String> {
        self.garbage_collect_orphans_at(grace_period, chrono::Utc::now())
            .await
    }

    /// Delete objects no manifest names any more, once they have been out of
    /// the active set for `grace_period`.
    ///
    /// What the grace is for, now that retirement unregisters before it writes
    /// the manifest and a reader can no longer plan a part the manifest has
    /// dropped:
    ///
    /// * a restore or merge already past its plan, holding descriptors from
    ///   the manifest it read;
    /// * local cache state a crash left behind, which a restart reconciles
    ///   against the store rather than against what it last had in memory;
    /// * a manifest consumer that is not this process at all;
    /// * a store whose listing lags its writes, where an object can be
    ///   absent from `active` because the manifest read was stale;
    /// * the gap between one lifecycle step and the next -- a publish whose
    ///   objects are up but whose manifest write has not landed is held by
    ///   the write-time half of the same check.
    ///
    /// So the value is a bound on how far behind the slowest of those may be,
    /// not a guess.
    pub(crate) async fn garbage_collect_orphans_at(
        &self,
        grace_period: std::time::Duration,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, String> {
        use futures_util::StreamExt;

        let manifest = self.load_manifest().await?;
        let trace_manifest = self.load_trace_manifest().await?;
        let metric_manifest = self.load_metric_manifest().await?;
        let mut active = HashSet::new();
        for part in &manifest.parts {
            for file in PART_FILES {
                active.insert(self.part_path(part, file).to_string());
            }
        }
        for part in &trace_manifest.parts {
            for file in TRACE_PART_FILES {
                active.insert(self.trace_part_path(part, file).to_string());
            }
        }
        for part in &metric_manifest.parts {
            for file in METRIC_PART_FILES {
                active.insert(self.metric_part_path(part, file).to_string());
            }
        }
        let cutoff = now
            - chrono::Duration::from_std(grace_period)
                .map_err(|error| format!("invalid garbage-collection grace period: {error}"))?;
        // When each object was first seen outside the active set. An object
        // retention retires is older than the grace by construction, so the
        // write time alone gives it no grace at all -- that check is what
        // keeps an upload that has not reached the manifest yet, and both have
        // to hold before anything is deleted.
        let mut first_orphaned = self.load_orphan_ledger().await?;
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        for prefix in [
            self.path("parts"),
            self.path("trace_parts"),
            self.path("metric_parts"),
        ] {
            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|error| format!("failed to list object store: {error}"))?;
                let key = meta.location.to_string();
                if active.contains(key.as_str()) {
                    // Back in the active set, so any grace it had started is
                    // no longer about anything.
                    first_orphaned.remove(&key);
                    continue;
                }
                seen.insert(key.clone());
                let orphaned_at = *first_orphaned.entry(key).or_insert(now);
                if meta.last_modified < cutoff && orphaned_at < cutoff {
                    candidates.push(meta.location);
                }
            }
        }
        // An object nobody lists any more takes its entry with it.
        first_orphaned.retain(|key, _| seen.contains(key));
        let mut removed = 0;
        for location in candidates {
            match self.store.delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    first_orphaned.remove(&location.to_string());
                    removed += 1;
                }
                Err(error) => {
                    return Err(format!(
                        "failed to delete orphan object {}: {error}",
                        location
                    ));
                }
            }
        }
        self.store_orphan_ledger(&first_orphaned).await?;
        Ok(removed)
    }

    /// The collector's own record of when each object left the active set.
    ///
    /// A first sighting is an upper bound on the retirement it stands for --
    /// the object left the manifest at or before the pass that noticed -- so
    /// reading it this way can only hold an object longer, never delete one
    /// early. It lives beside the manifests rather than under the part
    /// prefixes so that the collector never lists its own bookkeeping.
    async fn load_orphan_ledger(
        &self,
    ) -> Result<BTreeMap<String, chrono::DateTime<chrono::Utc>>, String> {
        let bytes = match self.store.get(&self.path(GC_ORPHANS_FILE)).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| format!("failed to read the orphan ledger: {error}"))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(BTreeMap::new()),
            Err(error) => return Err(format!("failed to load the orphan ledger: {error}")),
        };
        let raw: BTreeMap<String, String> = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid orphan ledger: {error}"))?;
        Ok(raw
            .into_iter()
            .filter_map(|(key, at)| {
                chrono::DateTime::parse_from_rfc3339(&at)
                    .ok()
                    .map(|at| (key, at.with_timezone(&chrono::Utc)))
            })
            .collect())
    }

    async fn store_orphan_ledger(
        &self,
        entries: &BTreeMap<String, chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), String> {
        let raw: BTreeMap<&str, String> = entries
            .iter()
            .map(|(key, at)| (key.as_str(), at.to_rfc3339()))
            .collect();
        let body = serde_json::to_vec(&raw)
            .map_err(|error| format!("failed to encode the orphan ledger: {error}"))?;
        self.store
            .put_opts(
                &self.path(GC_ORPHANS_FILE),
                body.into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("failed to store the orphan ledger: {error}"))?;
        Ok(())
    }

}
