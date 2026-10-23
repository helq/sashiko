// Copyright 2026 The Sashiko Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::db::{Database, FetchQueueRow};
use crate::events::{Event, MessageSource};
use crate::utils::redact_secret;
use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tracing::{error, info, warn};

/// How often the worker polls the durable queue for due work.
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// How often expired leases from dead workers are reclaimed.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// A claimed fetch whose worker stops updating it for this long is treated as a
/// ghost and reclaimed by the sweep.
const GHOST_LEASE_SECS: i64 = 600;
/// Total wall-clock window a fetch keeps retrying before it is failed.
const RETRY_WINDOW_SECS: i64 = 24 * 60 * 60;
/// First retry delay; subsequent delays double up to BACKOFF_CAP_SECS.
const BACKOFF_BASE_SECS: i64 = 30;
/// Maximum delay between retries.
const BACKOFF_CAP_SECS: i64 = 600;

/// Maximum number of fetch_queue rows to claim and pre-fetch in a single
/// `git fetch` call. Batching improves server-side pack negotiation.
const BATCH_SIZE: usize = 20;

/// Maximum number of commits allowed in a single range ingestion to prevent
/// unbounded processing and resource exhaustion from untrusted inputs.
const MAX_RANGE_COMMITS: usize = 256;

/// Maximum attempts for `git fetch` failures that return `HTTP 400` with
/// `expected 'packfile'` or `expected 'acknowledgments'` (server-side
/// upload-pack timeout / rejection) before treating the failure as terminal.
const MAX_HTTP_400_ATTEMPTS: i64 = 3;

/// Marker error for failures that can never succeed on retry (e.g. a commit
/// is missing and the row has no remote to fetch from). These bypass the
/// backoff retry window and fail the fetch immediately, mirroring the
/// fast-fail behaviour of the old in-memory FetchAgent.
#[derive(Debug)]
struct PermanentFetchError(String);

impl std::fmt::Display for PermanentFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PermanentFetchError {}

/// Returns true when `git fetch` stderr indicates the requested object or ref
/// permanently does not exist or is not allowed by the remote server.
fn is_permanent_git_fetch_stderr(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("not our ref")
        || lower.contains("unadvertised object")
        || lower.contains("couldn't find remote ref")
        || lower.contains("no such remote ref")
}

/// Returns true when a fetch failure message matches a Git-on-Borg `HTTP 400`
/// upload-pack timeout or rejection (`expected 'packfile'` or
/// `expected 'acknowledgments'`).
fn is_http_400_packfile_rejection(msg: &str) -> bool {
    msg.contains("HTTP 400")
        && (msg.contains("expected 'packfile'") || msg.contains("expected 'acknowledgments'"))
}

/// Derive the git commit or range to fetch from a placeholder message id.
///
/// Handles the two placeholder id shapes produced by the API:
///   - git-fetch:  "<sha>@sashiko.local"        -> "<sha>"
///   - PR/MR:      "mr-<number>-<base>..<head>"  -> "<base>..<head>"
pub fn commit_hash_from_placeholder(msgid: &str) -> String {
    // "mr-<number>-<rest>" -> "<rest>"; anything else passes through.
    let s = msgid
        .strip_prefix("mr-")
        .and_then(|rest| rest.find('-').map(|dash| &rest[dash + 1..]))
        .unwrap_or(msgid);
    // Strip any "@sashiko.local" suffix from git-fetch placeholder ids.
    s.strip_suffix("@sashiko.local").unwrap_or(s).to_string()
}

/// Current unix time in seconds.
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Exponential backoff (in seconds) for a given attempt count. `attempts` is the
/// number of attempts already made (>= 1 after the first claim): 1 -> 30s,
/// 2 -> 60s, 3 -> 120s, ... capped at BACKOFF_CAP_SECS.
fn backoff_secs(attempts: i64) -> i64 {
    let exp = attempts.saturating_sub(1).clamp(0, 20) as u32;
    BACKOFF_BASE_SECS
        .saturating_mul(2_i64.saturating_pow(exp))
        .min(BACKOFF_CAP_SECS)
}

/// Whether a revision string is a full git object id (sha1 = 40 hex chars, or
/// sha256 = 64). Only full object ids can be requested directly via `want`
/// without a ref lookup; branch/tag names must be resolved by the server.
fn looks_like_object_id(rev: &str) -> bool {
    matches!(rev.len(), 40 | 64) && rev.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Split a commit or commit-range specifier into its primary (`head`) endpoint
/// and an optional ancestor (`base`) endpoint.
///
/// For a single commit `sha`, returns `(sha, None)`. For `base..head` or
/// `base...head`, returns `(head, Some(base))` so callers can fetch `head`
/// first and only request `base` from the remote if it is still absent after
/// `head`'s packfile arrives.
pub fn parse_commit_range_phases(spec: &str) -> Result<(String, Option<String>)> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(anyhow!("Commit specifier must not be empty"));
    }

    let (start, end) = if let Some((start, end)) = spec.split_once("...") {
        (start.trim(), end.trim())
    } else if let Some((start, end)) = spec.split_once("..") {
        (start.trim(), end.trim())
    } else {
        return Ok((spec.to_string(), None));
    };

    if start.is_empty() || end.is_empty() {
        return Err(anyhow!(
            "Invalid range '{}': both start and end must be non-empty",
            spec
        ));
    }

    Ok((end.to_string(), Some(start.to_string())))
}

/// Extract commit endpoints from a commit specifier or range.
///
/// Both endpoints of a range must be non-empty (e.g. `base..head` or `base...head`).
/// Open-ended ranges (e.g. `..head`, `base..`, `...head`, `base...`) are invalid
/// and rejected with an error.
pub fn parse_commit_range_endpoints(spec: &str) -> Result<Vec<String>> {
    let (head, base) = parse_commit_range_phases(spec)?;
    match base {
        Some(b) => Ok(vec![b, head]),
        None => Ok(vec![head]),
    }
}

/// RAII guard that aborts a spawned Tokio task on drop.
struct TaskAbortGuard(tokio::task::JoinHandle<()>);

impl Drop for TaskAbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Durable, restart-safe git fetch worker.
///
/// Replaces the previous in-memory `FetchAgent` channel. Fetch requests live in
/// the `fetch_queue` table; this worker claims them under a lease, performs the
/// git fetch and patch extraction, and either completes them, schedules a
/// backoff retry, or (after RETRY_WINDOW_SECS) fails them terminally. Because
/// all state lives in the database, requests survive restarts and crashed
/// workers are recovered by the ghost sweep.
pub struct FetchWorker {
    repo_path: PathBuf,
    db: Arc<Database>,
    main_tx: mpsc::Sender<Event>,
    gitlab_token: Option<String>,
}

impl FetchWorker {
    pub fn new(
        repo_path: PathBuf,
        db: Arc<Database>,
        main_tx: mpsc::Sender<Event>,
        gitlab_token: Option<String>,
    ) -> Self {
        Self {
            repo_path,
            db,
            main_tx,
            gitlab_token,
        }
    }

    pub async fn run(self) {
        info!("FetchWorker started");

        // Reclaim ghost leases on an independent task so that a long-running (or
        // stuck) fetch on the main loop can never prevent dead-worker recovery.
        let mut sweeper = Self::spawn_sweeper(Arc::clone(&self.db));

        loop {
            if sweeper.0.is_finished() {
                if let Err(e) = (&mut sweeper.0).await {
                    error!(
                        "Ghost fetch sweeper task terminated unexpectedly: {}; respawning",
                        e
                    );
                }
                sweeper = Self::spawn_sweeper(Arc::clone(&self.db));
            }

            // Claim up to BATCH_SIZE rows and pre-fetch their objects in a
            // single `git fetch` call. Batching lets the server compute one
            // optimal pack for many wants, avoiding the pathological 11M-object
            // downloads that happen when unreachable SHAs are fetched one at a
            // time with poor negotiation. Retrying rows (attempts > 1 after
            // claim) are isolated into single-row batches so a slow or failing
            // retry cannot delay fresh rows in the same batch.
            let batch = self.claim_batch().await;
            if batch.is_empty() {
                tokio::select! {
                    _ = tokio::time::sleep(POLL_INTERVAL) => {}
                    res = &mut sweeper.0 => {
                        if let Err(e) = res {
                            error!(
                                "Ghost fetch sweeper task terminated unexpectedly: {}; respawning",
                                e
                            );
                        }
                        sweeper = Self::spawn_sweeper(Arc::clone(&self.db));
                    }
                }
                continue;
            }

            // Renew leases before prefetching so all rows have a fresh
            // lease window even if claiming the batch took time.
            for row in &batch {
                if let Err(e) = self.db.renew_fetch_lease(row.id, row.attempts).await {
                    warn!("Failed to renew initial lease for fetch {}: {}", row.id, e);
                }
            }

            // Spawn a periodic heartbeat task (every 60s) to keep leases alive
            // across both prefetch and batch processing. The TaskAbortGuard ensures
            // the task is always aborted when the batch scope ends (or on panic/early return).
            // active_idx tracks the index of the row currently being processed so
            // completed rows are retired before their DB state is cleared and never
            // emit false-positive lease-lost warnings.
            let db = Arc::clone(&self.db);
            let active_idx = Arc::new(AtomicUsize::new(0));
            let heartbeat_active_idx = Arc::clone(&active_idx);
            let mut leases: Vec<(usize, i64, i64)> = batch
                .iter()
                .enumerate()
                .map(|(idx, r)| (idx, r.id, r.attempts))
                .collect();
            let _heartbeat = TaskAbortGuard(tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(60));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let mut still_held = Vec::with_capacity(leases.len());
                    for (idx, id, attempts) in leases {
                        if idx < heartbeat_active_idx.load(Ordering::Acquire) {
                            continue;
                        }
                        match db.renew_fetch_lease(id, attempts).await {
                            Ok(true) => still_held.push((idx, id, attempts)),
                            Ok(false) => {
                                if idx >= heartbeat_active_idx.load(Ordering::Acquire) {
                                    warn!("Fetch lease lost during heartbeat for row {}", id);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to renew fetch lease for row {}: {}", id, e);
                                still_held.push((idx, id, attempts));
                            }
                        }
                    }
                    leases = still_held;
                    if leases.is_empty() {
                        break;
                    }
                }
            }));

            // Pre-fetch all missing objects in one batch call.
            self.batch_prefetch(&batch).await;

            // Process each row individually for ingestion.
            for (idx, row) in batch.into_iter().enumerate() {
                match self.db.renew_fetch_lease(row.id, row.attempts).await {
                    Ok(false) => {
                        active_idx.store(idx + 1, Ordering::Release);
                        warn!(
                            "Fetch lease for {} expired/lost to sweeper; skipping",
                            row.id
                        );
                        continue;
                    }
                    Err(e) => {
                        active_idx.store(idx + 1, Ordering::Release);
                        error!("Database error renewing fetch lease for {}: {}", row.id, e);
                        continue;
                    }
                    Ok(true) => {}
                }
                self.process_row(row, &active_idx, idx + 1).await;
            }
        }
    }

    /// Claim up to `BATCH_SIZE` due rows from the queue.
    ///
    /// Fresh rows (`attempts == 1` after claim) may be batched together up to
    /// `BATCH_SIZE`, whereas retrying rows (`attempts > 1` after claim) are
    /// isolated into single-row batches and never appended to a fresh batch.
    async fn claim_batch(&self) -> Vec<FetchQueueRow> {
        let mut batch = Vec::new();
        while batch.len() < BATCH_SIZE {
            let max_prev_attempts = if batch.is_empty() { None } else { Some(0) };
            match self
                .db
                .lock_pending_fetch_with_max_attempts(max_prev_attempts)
                .await
            {
                Ok(Some(row)) => {
                    let is_retry = row.attempts > 1;
                    batch.push(row);
                    if is_retry {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to claim pending fetch: {}", e);
                    break;
                }
            }
        }
        batch
    }

    fn spawn_sweeper(db: Arc<Database>) -> TaskAbortGuard {
        TaskAbortGuard(tokio::spawn(async move {
            let mut sweep = interval(SWEEP_INTERVAL);
            loop {
                sweep.tick().await;
                match db.sweep_ghost_fetches(GHOST_LEASE_SECS).await {
                    Ok(n) if n > 0 => {
                        warn!("Reclaimed {} ghost fetch(es) from dead workers", n);
                    }
                    Ok(_) => {}
                    Err(e) => error!("Ghost fetch sweep failed: {}", e),
                }
            }
        }))
    }

    /// Process a single claimed fetch: ensure the commits are present locally
    /// (fetching from the remote if needed), extract the patch(es), and emit the
    /// corresponding events. On any failure the row is retried with backoff, or
    /// failed terminally once the retry window is exhausted.
    async fn process_row(&self, row: FetchQueueRow, active_idx: &AtomicUsize, next_idx: usize) {
        info!(
            "Processing fetch {} (commit {}, attempt {})",
            row.id, row.commit_hash, row.attempts
        );

        if let Err(e) = self.ensure_present(&row).await {
            active_idx.store(next_idx, Ordering::Release);
            self.handle_failure(&row, e.context("fetch failed")).await;
            return;
        }

        let ingest_res = self.ingest(&row).await;
        active_idx.store(next_idx, Ordering::Release);

        match ingest_res {
            Ok(()) => {
                if let Err(e) = self.db.mark_fetch_done(row.id).await {
                    error!("Failed to mark fetch {} done: {}", row.id, e);
                } else {
                    info!("Fetch complete for {}", row.commit_hash);
                }
            }
            Err(e) => {
                self.handle_failure(&row, e.context("extract failed")).await;
            }
        }
    }

    /// Ensure every commit referenced by the row exists locally, fetching from
    /// the remote when necessary.
    async fn ensure_present(&self, row: &FetchQueueRow) -> Result<()> {
        // For ranges (base..head, base...head), split endpoints into primary
        // (head) and secondary (base) phases. Fetching head first brings its
        // ancestor history along in the same packfile, avoiding expensive
        // non-tip reachability walks on remotes with hundreds of thousands of
        // refs when base is already an ancestor of head.
        let (head, base) = parse_commit_range_phases(&row.commit_hash)
            .map_err(|e| PermanentFetchError(e.to_string()))?;
        let mut primary_to_check = vec![head];
        let mut base_to_check = Vec::new();
        if let Some(b) = base {
            base_to_check.push(b);
        }
        for sha in &row.supporting_commits {
            let (sup_head, sup_base) =
                parse_commit_range_phases(sha).map_err(|e| PermanentFetchError(e.to_string()))?;
            primary_to_check.push(sup_head);
            if let Some(b) = sup_base {
                base_to_check.push(b);
            }
        }

        for commit in primary_to_check.iter().chain(base_to_check.iter()) {
            let trimmed = commit.trim();
            if trimmed.is_empty()
                || trimmed.chars().any(char::is_whitespace)
                || trimmed.starts_with('-')
                || trimmed.starts_with('+')
                || trimmed.contains(':')
            {
                return Err(
                    PermanentFetchError(format!("Invalid commit reference: {}", commit)).into(),
                );
            }
        }

        let mut missing_primary = Vec::new();
        for commit in &primary_to_check {
            let trimmed = commit.trim();
            if !self.is_present(trimmed).await {
                missing_primary.push(trimmed.to_string());
            }
        }
        let mut missing_base = Vec::new();
        for commit in &base_to_check {
            let trimmed = commit.trim();
            if !self.is_present(trimmed).await {
                missing_base.push(trimmed.to_string());
            }
        }
        if missing_primary.is_empty() && missing_base.is_empty() {
            return Ok(());
        }

        let all_missing: Vec<String> = missing_primary
            .iter()
            .chain(missing_base.iter())
            .cloned()
            .collect();

        let url = match row.repo_url.as_deref().map(str::trim) {
            Some(u) if !u.is_empty() => u,
            _ => {
                return Err(PermanentFetchError(format!(
                    "commits {:?} missing and no remote is configured",
                    all_missing
                ))
                .into());
            }
        };

        // A local repository cannot be fetched from; the commits simply are not
        // there.
        let is_local = {
            let url_path = PathBuf::from(url);
            match (
                tokio::fs::canonicalize(&url_path).await,
                tokio::fs::canonicalize(&self.repo_path).await,
            ) {
                (Ok(a), Ok(b)) => a == b,
                _ => false,
            }
        };
        if is_local {
            return Err(PermanentFetchError(format!(
                "repository is local but commits are missing: {:?}",
                all_missing
            ))
            .into());
        }

        let remote_name = self.get_remote_name(url);

        // Acquire the per-remote lock so we never run concurrent fetches
        // against the same remote (shared with ensure_remote_with_refspecs
        // and batch_prefetch).
        let lock = crate::git_ops::get_remote_lock(&remote_name);
        let _guard = lock.lock().await;

        self.ensure_remote(&remote_name, url).await?;

        // Phase 1: Fetch missing primary (head) revisions first. We deliberately
        // do NOT fall back to a full `git fetch` of all refs: large mirrors
        // (e.g. the kernel repo with hundreds of thousands of refs) make that
        // pathologically slow, and it is unnecessary -- a still-missing object
        // here is genuinely unfetchable and should surface as an error.
        let mut still_missing_primary = Vec::new();
        for commit in &missing_primary {
            if !self.is_present(commit).await {
                still_missing_primary.push(commit.clone());
            }
        }
        if !still_missing_primary.is_empty() {
            self.fetch_commits(&remote_name, &still_missing_primary)
                .await?;
        }

        // Phase 2: Re-check any missing base endpoints after Phase 1. For normal
        // ancestor ranges (base..head), fetching head already downloaded base in
        // the packfile. Only fetch base explicitly if it remains absent (e.g. a
        // divergent three-dot range base...head).
        let mut still_missing_base = Vec::new();
        for commit in &missing_base {
            if !self.is_present(commit).await {
                still_missing_base.push(commit.clone());
            }
        }
        if !still_missing_base.is_empty() {
            self.fetch_commits(&remote_name, &still_missing_base)
                .await?;
        }

        for commit in &all_missing {
            if !self.is_present(commit).await {
                return Err(PermanentFetchError(format!(
                    "commit {} still missing after fetch from {}",
                    commit,
                    redact_secret(url)
                ))
                .into());
            }
        }
        Ok(())
    }

    /// Extract patch(es) for the row and emit PatchSubmitted events.
    async fn ingest(&self, row: &FetchQueueRow) -> Result<()> {
        let article_id = Self::article_id_for(row);
        let mr_url = row.mr_url.clone();
        let mr_title = row.mr_title.clone();

        if row.commit_hash.contains("..") {
            let shas = crate::git_ops::resolve_git_range_bounded(
                &self.repo_path,
                &row.commit_hash,
                Some(MAX_RANGE_COMMITS + 1),
            )
            .await?;
            if shas.is_empty() {
                return Err(PermanentFetchError(format!(
                    "Commit range {} resolved to zero commits",
                    row.commit_hash
                ))
                .into());
            }
            if shas.len() > MAX_RANGE_COMMITS {
                return Err(PermanentFetchError(format!(
                    "git range '{}' contains {} commits, exceeding maximum of {}",
                    row.commit_hash,
                    shas.len(),
                    MAX_RANGE_COMMITS
                ))
                .into());
            }
            let count = shas.len() as u32;
            for (i, sha) in shas.iter().enumerate() {
                let mut event = self
                    .extract_patch(
                        sha,
                        &article_id,
                        (i + 1) as u32,
                        count,
                        mr_url.as_ref(),
                        mr_title.as_ref(),
                        row.mr_number,
                    )
                    .await?;
                if let Event::PatchSubmitted {
                    ref mut message_id, ..
                } = event
                {
                    *message_id = sha.clone();
                }
                self.main_tx
                    .send(event)
                    .await
                    .map_err(|e| anyhow!("failed to send event: {}", e))?;
            }
            info!("Submitted range {}", row.commit_hash);
        } else {
            let full_sha = self.resolve_sha(&row.commit_hash).await?;
            if row.patchset_id.is_none() && self.db.has_patchset_by_msgid(&full_sha).await? {
                info!("Skipping already ingested single patch {}", full_sha);
                return Ok(());
            }
            let mut event = self
                .extract_patch(
                    &full_sha,
                    &article_id,
                    1,
                    1,
                    mr_url.as_ref(),
                    mr_title.as_ref(),
                    row.mr_number,
                )
                .await?;
            if let Event::PatchSubmitted {
                ref mut message_id, ..
            } = event
            {
                *message_id = full_sha.clone();
            }
            self.main_tx
                .send(event)
                .await
                .map_err(|e| anyhow!("failed to send event: {}", e))?;
            info!("Submitted patch {}", row.commit_hash);
        }
        Ok(())
    }

    /// Decide whether to retry (with backoff) or terminally fail a fetch.
    async fn handle_failure(&self, row: &FetchQueueRow, error: anyhow::Error) {
        let now = now_secs();
        let first = row.first_attempt_at.unwrap_or(now);
        let permanent = error.chain().any(|e| e.is::<PermanentFetchError>());
        let msg = format!("{:#}", error);
        let http_400_exhausted =
            row.attempts >= MAX_HTTP_400_ATTEMPTS && is_http_400_packfile_rejection(&msg);
        if permanent {
            error!(
                "Fetch {} for {} failed permanently (not retrying): {}",
                row.id, row.commit_hash, msg
            );
        } else if http_400_exhausted {
            error!(
                "Fetch {} for {} failed after {} HTTP 400 upload-pack attempts (not retrying): {}",
                row.id, row.commit_hash, row.attempts, msg
            );
        } else if now - first >= RETRY_WINDOW_SECS {
            error!(
                "Fetch {} for {} exhausted its {}h retry window: {}",
                row.id,
                row.commit_hash,
                RETRY_WINDOW_SECS / 3600,
                msg
            );
        }
        if permanent || http_400_exhausted || now - first >= RETRY_WINDOW_SECS {
            if let Err(e) = self.db.mark_fetch_failed(row.id, &msg).await {
                error!("Failed to mark fetch {} failed: {}", row.id, e);
            }
            // Surface the terminal failure so the placeholder patchset moves out
            // of 'Fetching' into 'Failed'.
            if let Err(e) = self
                .main_tx
                .send(Event::IngestionFailed {
                    article_id: Self::article_id_for(row),
                    error: msg.clone(),
                    source: MessageSource::GitFetch,
                })
                .await
            {
                warn!(
                    "Failed to send IngestionFailed event for fetch {}: {}",
                    row.id, e
                );
            }
        } else {
            let backoff = backoff_secs(row.attempts);
            warn!(
                "Fetch {} for {} failed (attempt {}), retrying in {}s: {}",
                row.id, row.commit_hash, row.attempts, backoff, msg
            );
            if let Err(e) = self
                .db
                .set_fetch_retry_at(row.id, now + backoff, &msg)
                .await
            {
                error!("Failed to schedule retry for fetch {}: {}", row.id, e);
            }
        }
    }

    fn article_id_for(row: &FetchQueueRow) -> String {
        match row.mr_number {
            Some(number) => format!("mr-{}-{}", number, row.commit_hash),
            None => row.commit_hash.clone(),
        }
    }
    fn get_remote_name(&self, url: &str) -> String {
        // Use a hash of the URL to ensure safe and unique remote names
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        url.hash(&mut hasher);
        format!("fetcher-{:x}", hasher.finish())
    }

    async fn ensure_remote(&self, name: &str, url: &str) -> Result<()> {
        let global_lock = crate::git_ops::get_global_config_lock();
        let _global_guard = global_lock.lock().await;

        // Inject GitLab token via parsed url::Url to prevent parser differentials.
        let authenticated_url = if let Some(token) = &self.gitlab_token {
            if let Ok(mut parsed) = url::Url::parse(url)
                && parsed.scheme() == "https"
                && parsed
                    .host_str()
                    .is_some_and(|h| h.eq_ignore_ascii_case("gitlab.com"))
            {
                let _ = parsed.set_username("oauth2");
                let _ = parsed.set_password(Some(token));
                parsed.to_string()
            } else {
                url.to_string()
            }
        } else {
            url.to_string()
        };

        // Check if remote exists
        let status = crate::git_cmd::in_dir_async(&self.repo_path)
            .args(["-c", "safe.bareRepository=all"])
            .args(["remote", "get-url", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;

        if status.success() {
            let output = crate::git_cmd::in_dir_async(&self.repo_path)
                .args(["-c", "safe.bareRepository=all"])
                .args(["remote", "get-url", name])
                .output()
                .await?;
            let current_url = String::from_utf8_lossy(&output.stdout).trim().to_string();

            if current_url != authenticated_url {
                info!(
                    "Updating remote {} from {} to {}",
                    name,
                    redact_secret(&current_url),
                    redact_secret(&authenticated_url)
                );
                let output = crate::git_cmd::in_dir_async(&self.repo_path)
                    .args(["-c", "safe.bareRepository=all"])
                    .args(["remote", "set-url", name, &authenticated_url])
                    .output()
                    .await?;

                if !output.status.success() {
                    return Err(anyhow!(
                        "Failed to update remote: {}",
                        redact_secret(String::from_utf8_lossy(&output.stderr).trim())
                    ));
                }
            }
        } else {
            info!(
                "Adding remote {} -> {}",
                name,
                redact_secret(&authenticated_url)
            );
            let output = crate::git_cmd::in_dir_async(&self.repo_path)
                .args(["-c", "safe.bareRepository=all"])
                .args(["remote", "add", name, &authenticated_url])
                .output()
                .await?;

            if !output.status.success() {
                return Err(anyhow!(
                    "Failed to add remote: {}",
                    redact_secret(String::from_utf8_lossy(&output.stderr).trim())
                ));
            }
        }
        Ok(())
    }

    /// Base `git fetch` command shared by all fetches: no interactive prompt,
    /// the standard protocol restrictions, and `kill_on_drop` so a cancelled
    /// fetch is reaped. Callers append fetch flags, the remote, and revisions.
    ///
    /// Note: protocol v2 is deliberately NOT forced here. Protocol v1 sends
    /// all remote refs upfront, giving the client rich negotiation data that
    /// avoids pathological full-repo downloads for unreachable SHAs.
    fn fetch_base_cmd(&self) -> tokio::process::Command {
        let mut cmd = crate::git_cmd::in_dir_async(&self.repo_path);
        cmd.args(["-c", "safe.bareRepository=all"])
            .env("GIT_TERMINAL_PROMPT", "0")
            .args(crate::git_ops::GIT_PROTOCOL_RESTRICTIONS)
            .arg("fetch")
            .kill_on_drop(true);
        cmd
    }

    /// Runs one `git fetch`, dropping a stale commit-graph and trying
    /// again when that is what turned the fetch away.
    async fn fetch_with_graph_retry(&self, args: &[&str]) -> Result<Output> {
        let mut dropped_graph = false;

        loop {
            let mut cmd = self.fetch_base_cmd();
            cmd.args(args);
            // DO NOT wrap this fetch in a timeout. Network transfer and
            // `git index-pack` delta resolution for large repositories (e.g.
            // internal kernel build trees with hundreds of thousands of
            // objects) legitimately take significant time. Imposing a timeout
            // drops the child process, triggering SIGKILL via `kill_on_drop`,
            // which causes git to discard the partial pack and forces the
            // fetch worker into an endless retry loop.
            let output = cmd.output().await?;

            if output.status.success() || dropped_graph {
                if dropped_graph {
                    crate::git_ops::schedule_commit_graph_rebuild(&self.repo_path);
                }
                return Ok(output);
            }

            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            if !crate::git_ops::is_stale_commit_graph(&stderr) {
                return Ok(output);
            }

            warn!("Fetch found a stale commit-graph; dropping it");
            if let Err(e) = crate::git_ops::drop_commit_graph(&self.repo_path).await {
                warn!("Failed to drop the commit-graph: {}", e);
                return Ok(output);
            }
            dropped_graph = true;
        }
    }

    /// Fetch the given revisions from `remote`, tuned for a very large remote.
    ///
    /// Full object ids are fetched directly (`want <oid>`) in a single batched
    /// call with no ref advertisement. Branch/tag names are resolved server-side
    /// via a protocol-v2, prefix-filtered `ls-refs` (cheap even against a remote
    /// with hundreds of thousands of refs) and fetched individually so one bad
    /// name does not fail the whole batch.
    async fn fetch_commits(&self, remote: &str, commits: &[String]) -> Result<()> {
        let mut object_ids: Vec<&str> = Vec::new();
        let mut ref_names: Vec<&str> = Vec::new();
        for c in commits {
            if c.is_empty()
                || c.chars().any(char::is_whitespace)
                || c.starts_with('-')
                || c.starts_with('+')
                || c.contains(':')
            {
                warn!("Skipping invalid commit ref in fetch batch: {}", c);
                continue;
            }
            if looks_like_object_id(c) {
                object_ids.push(c.as_str());
            } else {
                ref_names.push(c.as_str());
            }
        }

        if object_ids.is_empty() && ref_names.is_empty() {
            return Err(anyhow!("No valid commit refs to fetch"));
        }

        if !object_ids.is_empty() {
            let mut args = vec![
                "--no-tags",
                "--no-write-fetch-head",
                "--end-of-options",
                remote,
            ];
            args.extend(object_ids.iter().copied());
            let output = self.fetch_with_graph_retry(&args).await?;
            if !output.status.success() {
                let redacted = redact_secret(String::from_utf8_lossy(&output.stderr).trim());
                let msg = format!(
                    "fetch of {} object(s) failed: {}",
                    object_ids.len(),
                    redacted
                );
                if is_permanent_git_fetch_stderr(&redacted) {
                    return Err(PermanentFetchError(msg).into());
                }
                return Err(anyhow!(msg));
            }
        }

        let mut first_err = None;
        for name in ref_names {
            let output = self
                .fetch_with_graph_retry(&[
                    "--no-tags",
                    "--no-write-fetch-head",
                    "--end-of-options",
                    remote,
                    name,
                ])
                .await?;
            if !output.status.success() {
                let redacted = redact_secret(String::from_utf8_lossy(&output.stderr).trim());
                let msg = format!("fetch of ref {} failed: {}", name, redacted);
                let err = if is_permanent_git_fetch_stderr(&redacted) {
                    PermanentFetchError(msg).into()
                } else {
                    anyhow!(msg)
                };
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    /// Pre-fetch objects for a batch of rows in a single `git fetch` call.
    /// Groups rows by remote URL and fetches all missing SHAs together so
    /// the server can compute one optimal pack.
    ///
    /// Remotes where fewer than two rows have missing commits are skipped so a
    /// single failing row is not fetched twice back-to-back (once in
    /// `batch_prefetch` and immediately again in `ensure_present`).
    async fn batch_prefetch(&self, rows: &[FetchQueueRow]) {
        if rows.len() < 2 {
            return;
        }

        // Group missing primary (head) SHAs and contributing row count by
        // remote URL. For commit ranges (base..head, base...head), only
        // prefetch head: fetching head brings ancestor base commits
        // automatically in the packfile without forcing the remote into a
        // non-tip reachability scan for base. Any divergent base still missing
        // after prefetch is fetched by ensure_present.
        let mut by_remote: std::collections::HashMap<String, (usize, Vec<String>)> =
            std::collections::HashMap::new();
        for row in rows {
            let url = row.repo_url.as_deref().unwrap_or("").trim().to_string();
            if url.is_empty() {
                continue;
            }
            let mut commits = match parse_commit_range_phases(&row.commit_hash) {
                Ok((head, _base)) => vec![head],
                Err(e) => {
                    warn!(
                        "batch_prefetch: skipping invalid range in row {}: {}",
                        row.id, e
                    );
                    continue;
                }
            };
            for sha in &row.supporting_commits {
                match parse_commit_range_phases(sha) {
                    Ok((head, _base)) => commits.push(head),
                    Err(e) => {
                        warn!(
                            "batch_prefetch: skipping invalid supporting commit in row {}: {}",
                            row.id, e
                        );
                    }
                }
            }

            let mut row_missing = Vec::new();
            for c in commits {
                let trimmed = c.trim();
                if trimmed.is_empty()
                    || trimmed.chars().any(char::is_whitespace)
                    || trimmed.starts_with('-')
                    || trimmed.starts_with('+')
                    || trimmed.contains(':')
                {
                    continue;
                }
                if !self.is_present(trimmed).await {
                    row_missing.push(trimmed.to_string());
                }
            }
            if !row_missing.is_empty() {
                let entry = by_remote.entry(url).or_insert_with(|| (0, Vec::new()));
                entry.0 += 1;
                entry.1.extend(row_missing);
            }
        }

        for (url, (row_count, missing)) in &mut by_remote {
            if *row_count < 2 {
                continue;
            }
            missing.sort();
            missing.dedup();
            if missing.is_empty() {
                continue;
            }

            let remote_name = self.get_remote_name(url);
            if let Err(e) = self.ensure_remote(&remote_name, url).await {
                warn!(
                    "batch_prefetch: failed to ensure remote {}: {}",
                    redact_secret(url),
                    e
                );
                continue;
            }
            info!(
                "batch_prefetch: fetching {} SHAs across {} rows from {} in one call",
                missing.len(),
                row_count,
                redact_secret(url)
            );
            // Acquire the per-remote lock so we never run concurrent fetches
            // against the same remote.
            let lock = crate::git_ops::get_remote_lock(&remote_name);
            let _guard = lock.lock().await;
            if let Err(e) = self.fetch_commits(&remote_name, missing).await {
                warn!("batch_prefetch: batch fetch failed: {}", e);
            }
        }
    }

    async fn is_present(&self, commit: &str) -> bool {
        let commit = commit.trim();
        if commit.starts_with('-') {
            return false;
        }
        let arg_str = format!("{}^{{commit}}", commit);
        let output = crate::git_cmd::in_dir_async(&self.repo_path)
            .args([
                "-c",
                "safe.bareRepository=all",
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                &arg_str,
            ])
            .output()
            .await;

        match output {
            Ok(s) => {
                let success = s.status.success();
                if success {
                    info!("is_present: {} is present", commit);
                } else {
                    info!(
                        "is_present: {} is missing or not a commit. stderr: {}",
                        commit,
                        String::from_utf8_lossy(&s.stderr)
                    );
                }
                success
            }
            Err(e) => {
                error!("is_present: {} check failed: {}", commit, e);
                false
            }
        }
    }

    async fn resolve_sha(&self, commit: &str) -> Result<String> {
        let commit = commit.trim();
        if commit.starts_with('-') {
            return Err(PermanentFetchError(format!(
                "Invalid commit reference starting with '-': {}",
                commit
            ))
            .into());
        }
        let arg_str = format!("{}^{{commit}}", commit);
        let output = crate::git_cmd::in_dir_async(&self.repo_path)
            .args(["-c", "safe.bareRepository=all"])
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                &arg_str,
            ])
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow!(
                "Failed to resolve SHA for {}: {}",
                commit,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    #[allow(clippy::too_many_arguments)]
    async fn extract_patch(
        &self,
        commit: &str,
        article_id: &str,
        index: u32,
        total: u32,
        mr_url: Option<&String>,
        mr_title: Option<&String>,
        mr_number: Option<i64>,
    ) -> Result<Event> {
        let meta = crate::git_ops::extract_patch_metadata(&self.repo_path, commit).await?;

        Ok(Event::PatchSubmitted {
            group: "git-fetch".to_string(),
            article_id: article_id.to_string(),
            message_id: String::new(), // Set by caller
            subject: meta.subject,
            author: meta.author,
            message: meta.message,
            diff: meta.diff,
            base_commit: meta.base_commit,
            timestamp: meta.timestamp,
            index,
            total,
            mr_url: mr_url.cloned(),
            mr_title: mr_title.cloned(),
            mr_number,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    async fn test_worker(repo_path: PathBuf) -> FetchWorker {
        let settings = crate::settings::DatabaseSettings {
            url: ":memory:".to_string(),
            token: String::new(),
        };
        let db = Arc::new(Database::new(&settings).await.unwrap());
        db.migrate().await.unwrap();
        let (tx, _rx) = mpsc::channel(1);
        FetchWorker::new(repo_path, db, tx, None)
    }

    #[tokio::test]
    async fn test_fetch_worker_construction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let _worker = test_worker(temp_dir.path().to_path_buf()).await;
    }

    #[tokio::test]
    async fn test_extract_patch_parsing() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();

        // Setup dummy repo
        crate::git_cmd::in_dir_async(&repo_path)
            .arg("init")
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;

        let file_path = repo_path.join("file.txt");
        let mut file = File::create(&file_path)?;
        writeln!(file, "content")?;

        crate::git_cmd::in_dir_async(&repo_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit", "-m", "Subject Line\n\nBody Line"])
            .output()
            .await?;

        let worker = test_worker(repo_path.clone()).await;

        let output = crate::git_cmd::in_dir_async(&repo_path)
            .args(["rev-parse", "HEAD"])
            .output()
            .await?;
        let head = String::from_utf8(output.stdout)?.trim().to_string();

        let event = worker
            .extract_patch(&head, &head, 1, 1, None, None, None)
            .await?;

        match event {
            Event::PatchSubmitted {
                subject,
                author,
                message,
                diff,
                article_id,
                ..
            } => {
                assert_eq!(subject, "Subject Line");
                assert_eq!(author, "Test User <test@example.com>");
                assert!(message.contains("Body Line"));
                assert!(diff.contains("diff --git"));
                assert_eq!(article_id, head);
            }
            _ => panic!("Wrong event type"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_present_with_tree_sha() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();

        // Setup dummy repo
        crate::git_cmd::in_dir_async(&repo_path)
            .arg("init")
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;

        let file_path = repo_path.join("file.txt");
        let mut file = File::create(&file_path)?;
        writeln!(file, "content")?;

        crate::git_cmd::in_dir_async(&repo_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit", "-m", "Subject Line"])
            .output()
            .await?;

        let worker = test_worker(repo_path.clone()).await;

        let output = crate::git_cmd::in_dir_async(&repo_path)
            .args(["rev-parse", "HEAD^{tree}"])
            .output()
            .await?;
        let tree_sha = String::from_utf8(output.stdout)?.trim().to_string();

        assert!(
            !worker.is_present(&tree_sha).await,
            "Tree SHA should not be considered a present commit"
        );

        let output = crate::git_cmd::in_dir_async(&repo_path)
            .args(["rev-parse", "HEAD"])
            .output()
            .await?;
        let commit_sha = String::from_utf8(output.stdout)?.trim().to_string();

        assert!(
            worker.is_present(&commit_sha).await,
            "Commit SHA should be considered present"
        );

        Ok(())
    }

    #[test]
    fn test_commit_hash_from_placeholder() {
        assert_eq!(
            commit_hash_from_placeholder("abc123@sashiko.local"),
            "abc123"
        );
        assert_eq!(
            commit_hash_from_placeholder("mr-42-base..head"),
            "base..head"
        );
        assert_eq!(
            commit_hash_from_placeholder("mr-42-base..head@sashiko.local"),
            "base..head"
        );
        assert_eq!(
            commit_hash_from_placeholder("feature@v2@sashiko.local"),
            "feature@v2"
        );
        assert_eq!(commit_hash_from_placeholder("feature@v2"), "feature@v2");
        assert_eq!(
            commit_hash_from_placeholder("mr-42-feat@v1..feat@v2@sashiko.local"),
            "feat@v1..feat@v2"
        );
        assert_eq!(commit_hash_from_placeholder("plainsha"), "plainsha");
    }

    #[test]
    fn test_backoff_secs_is_exponential_and_capped() {
        assert_eq!(backoff_secs(1), 30);
        assert_eq!(backoff_secs(2), 60);
        assert_eq!(backoff_secs(3), 120);
        assert_eq!(backoff_secs(4), 240);
        assert_eq!(backoff_secs(5), 480);
        assert_eq!(backoff_secs(6), 600);
        assert_eq!(backoff_secs(100), 600);
    }

    #[test]
    fn test_looks_like_object_id() {
        assert!(looks_like_object_id(&"a".repeat(40)));
        assert!(looks_like_object_id(&"0".repeat(64)));
        assert!(looks_like_object_id(
            "8a21aa5149b3f34f48a277d596d1ffe512b32a41"
        ));
        assert!(!looks_like_object_id("main"));
        assert!(!looks_like_object_id("v6.10"));
        assert!(!looks_like_object_id("refs/heads/main"));
        assert!(!looks_like_object_id(&"a".repeat(39)));
        assert!(!looks_like_object_id("zzzz"));
    }

    #[test]
    fn test_parse_commit_range_endpoints() {
        assert_eq!(
            parse_commit_range_endpoints("abc1234").unwrap(),
            vec!["abc1234"]
        );
        assert_eq!(
            parse_commit_range_endpoints("v1.0..v2.0").unwrap(),
            vec!["v1.0", "v2.0"]
        );
        assert_eq!(
            parse_commit_range_endpoints("v1.0...v2.0").unwrap(),
            vec!["v1.0", "v2.0"]
        );
        assert!(parse_commit_range_endpoints("").is_err());
        assert!(parse_commit_range_endpoints("..head").is_err());
        assert!(parse_commit_range_endpoints("base..").is_err());
        assert!(parse_commit_range_endpoints("...head").is_err());
        assert!(parse_commit_range_endpoints("base...").is_err());
    }

    #[test]
    fn test_handle_failure_detects_wrapped_permanent_fetch_error() {
        let err: anyhow::Error = PermanentFetchError("fatal error".to_string()).into();
        let wrapped = err.context("extract failed");
        assert!(wrapped.chain().any(|e| e.is::<PermanentFetchError>()));
    }

    #[tokio::test]
    async fn test_resolve_sha_leading_dash_rejected() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;

        let worker = test_worker(repo_path).await;
        let err = worker.resolve_sha("--all").await.unwrap_err();
        assert!(
            err.downcast_ref::<PermanentFetchError>().is_some(),
            "Expected PermanentFetchError, got: {:?}",
            err
        );
        assert!(err.to_string().contains("starting with '-'"));
        Ok(())
    }

    #[tokio::test]
    async fn test_git_fetch_separator_behavior() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let remote_path = temp_dir.path().join("remote");
        let local_path = temp_dir.path().join("local");
        std::fs::create_dir_all(&remote_path)?;
        std::fs::create_dir_all(&local_path)?;

        crate::git_cmd::in_dir_async(&remote_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.name", "Test"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.email", "test@test.com"])
            .output()
            .await?;
        std::fs::write(remote_path.join("file.txt"), "hello\n")?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["commit", "-m", "init"])
            .output()
            .await?;

        crate::git_cmd::in_dir_async(&local_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&local_path)
            .args(["remote", "add", "origin", remote_path.to_str().unwrap()])
            .output()
            .await?;

        // 1. git fetch with --end-of-options
        let out_eoo = crate::git_cmd::in_dir_async(&local_path)
            .args([
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--end-of-options",
                "origin",
                "main",
            ])
            .output()
            .await?;
        assert!(out_eoo.status.success());

        // 2. git fetch with leading dash remote name is blocked by --end-of-options
        let out_blocked = crate::git_cmd::in_dir_async(&local_path)
            .args(["fetch", "--end-of-options", "--upload-pack=false", "main"])
            .output()
            .await?;
        assert!(!out_blocked.status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_process_row_advances_active_idx_before_completion() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@test.com"])
            .output()
            .await?;
        std::fs::write(repo_path.join("file.txt"), "hello\n")?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit", "-m", "init"])
            .output()
            .await?;

        let head = String::from_utf8(
            crate::git_cmd::in_dir_async(&repo_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        let settings = crate::settings::DatabaseSettings {
            url: ":memory:".to_string(),
            token: String::new(),
        };
        let db = Arc::new(Database::new(&settings).await?);
        db.migrate().await?;
        let (tx, _rx) = mpsc::channel(10);
        let worker = FetchWorker::new(repo_path, Arc::clone(&db), tx, None);

        db.enqueue_fetch(None, Some("cover-ok"), None, &head, None, None, None, 500)
            .await?;
        db.enqueue_fetch(
            None,
            Some("cover-err"),
            None,
            "--invalid-ref",
            None,
            None,
            None,
            500,
        )
        .await?;

        let row0 = db.lock_pending_fetch().await?.unwrap();
        let row1 = db.lock_pending_fetch().await?.unwrap();
        let row0_id = row0.id;
        let row0_attempts = row0.attempts;
        let row1_id = row1.id;
        let row1_attempts = row1.attempts;

        let active_idx = AtomicUsize::new(0);

        worker.process_row(row0, &active_idx, 1).await;
        assert_eq!(active_idx.load(Ordering::Acquire), 1);
        assert!(!db.renew_fetch_lease(row0_id, row0_attempts).await?);
        assert!(db.renew_fetch_lease(row1_id, row1_attempts).await?);

        worker.process_row(row1, &active_idx, 2).await;
        assert_eq!(active_idx.load(Ordering::Acquire), 2);
        assert!(!db.renew_fetch_lease(row1_id, row1_attempts).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_prefetch_skips_single_row_remote() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let remote_path = temp_dir.path().join("remote");
        let local_path = temp_dir.path().join("local");
        std::fs::create_dir_all(&remote_path)?;
        std::fs::create_dir_all(&local_path)?;

        crate::git_cmd::in_dir_async(&remote_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;

        std::fs::write(remote_path.join("file.txt"), "c1\n")?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["commit", "-m", "commit 1"])
            .output()
            .await?;
        let sha1 = String::from_utf8(
            crate::git_cmd::in_dir_async(&remote_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        std::fs::write(remote_path.join("file.txt"), "c2\n")?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["commit", "-m", "commit 2"])
            .output()
            .await?;
        let sha2 = String::from_utf8(
            crate::git_cmd::in_dir_async(&remote_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        crate::git_cmd::in_dir_async(&local_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        let worker = test_worker(local_path).await;
        let remote_url = remote_path.to_str().unwrap().to_string();

        let row1 = FetchQueueRow {
            id: 1,
            patchset_id: None,
            cover_letter_message_id: None,
            repo_url: Some(remote_url.clone()),
            commit_hash: sha1.clone(),
            mr_url: None,
            mr_title: None,
            mr_number: None,
            status: crate::db::FetchStatus::Fetching,
            attempts: 1,
            first_attempt_at: None,
            next_retry_at: None,
            locked_at: None,
            last_error: None,
            priority: 500,
            created_at: 0,
            supporting_commits: Vec::new(),
        };
        let row2 = FetchQueueRow {
            id: 2,
            commit_hash: sha2.clone(),
            ..row1.clone()
        };

        // A single row for a remote must not trigger batch_prefetch (avoiding
        // duplicate fetches when ensure_present runs next).
        worker.batch_prefetch(std::slice::from_ref(&row1)).await;
        assert!(!worker.is_present(&sha1).await);

        // Two rows needing the same remote trigger batch_prefetch in one call.
        worker.batch_prefetch(&[row1, row2]).await;
        assert!(worker.is_present(&sha1).await);
        assert!(worker.is_present(&sha2).await);

        Ok(())
    }

    #[tokio::test]
    async fn test_claim_batch_isolates_retries_from_fresh_rows() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let worker = test_worker(temp_dir.path().to_path_buf()).await;

        // Enqueue two rows that fail once and become due retries (attempts = 1).
        worker
            .db
            .enqueue_fetch(None, None, Some("repo"), "retry-1", None, None, None, 600)
            .await?;
        worker
            .db
            .enqueue_fetch(None, None, Some("repo"), "retry-2", None, None, None, 600)
            .await?;
        let r1 = worker.db.lock_pending_fetch().await?.unwrap();
        let r2 = worker.db.lock_pending_fetch().await?.unwrap();
        worker.db.set_fetch_retry_at(r1.id, 1, "transient").await?;
        worker.db.set_fetch_retry_at(r2.id, 1, "transient").await?;

        // Enqueue two fresh rows (attempts = 0).
        worker
            .db
            .enqueue_fetch(None, None, Some("repo"), "fresh-1", None, None, None, 500)
            .await?;
        worker
            .db
            .enqueue_fetch(None, None, Some("repo"), "fresh-2", None, None, None, 500)
            .await?;

        // First batch must contain ONLY the two fresh rows, leaving the due
        // retries unclaimed rather than mixing them into the fresh batch.
        let batch1 = worker.claim_batch().await;
        let hashes1: Vec<&str> = batch1.iter().map(|r| r.commit_hash.as_str()).collect();
        assert_eq!(hashes1, vec!["fresh-1", "fresh-2"]);

        // Subsequent batches claim the due retry rows one at a time (batch size 1).
        let batch2 = worker.claim_batch().await;
        let hashes2: Vec<&str> = batch2.iter().map(|r| r.commit_hash.as_str()).collect();
        assert_eq!(hashes2, vec!["retry-1"]);

        let batch3 = worker.claim_batch().await;
        let hashes3: Vec<&str> = batch3.iter().map(|r| r.commit_hash.as_str()).collect();
        assert_eq!(hashes3, vec!["retry-2"]);

        assert!(worker.claim_batch().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_worker_drains_batches_without_poll_stall() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@test.com"])
            .output()
            .await?;
        std::fs::write(repo_path.join("file.txt"), "hello\n")?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit", "-m", "init"])
            .output()
            .await?;

        let head = String::from_utf8(
            crate::git_cmd::in_dir_async(&repo_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        let settings = crate::settings::DatabaseSettings {
            url: ":memory:".to_string(),
            token: String::new(),
        };
        let db = Arc::new(Database::new(&settings).await?);
        db.migrate().await?;
        let (tx, mut rx) = mpsc::channel(100);
        let worker = FetchWorker::new(repo_path, Arc::clone(&db), tx, None);

        // Enqueue BATCH_SIZE + 2 items (22 items)
        for i in 0..(BATCH_SIZE + 2) {
            db.enqueue_fetch(
                None,
                Some(&format!("cover-{}", i)),
                None,
                &head,
                None,
                None,
                None,
                500,
            )
            .await?;
        }

        // Spawn worker.run()
        let worker_task = tokio::spawn(async move {
            worker.run().await;
        });

        let mut count = 0;
        while count < BATCH_SIZE + 2 {
            match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
                Ok(Some(Event::PatchSubmitted { .. })) => count += 1,
                _ => break,
            }
        }
        assert_eq!(
            count,
            BATCH_SIZE + 2,
            "Worker should have drained all 22 items across 2 batches immediately"
        );
        assert!(db.lock_pending_fetch().await?.is_none());

        worker_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_present_checks_supporting_commits() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();

        crate::git_cmd::in_dir_async(&repo_path)
            .arg("init")
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;

        let file_path = repo_path.join("file.txt");
        let mut file = File::create(&file_path)?;
        writeln!(file, "content")?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit", "-m", "Commit 1"])
            .output()
            .await?;

        let output = crate::git_cmd::in_dir_async(&repo_path)
            .args(["rev-parse", "HEAD"])
            .output()
            .await?;
        let commit1 = String::from_utf8(output.stdout)?.trim().to_string();

        let worker = test_worker(repo_path.clone()).await;

        let row = FetchQueueRow {
            id: 1,
            patchset_id: None,
            cover_letter_message_id: None,
            repo_url: None,
            commit_hash: commit1.clone(),
            mr_url: None,
            mr_title: None,
            mr_number: None,
            status: crate::db::FetchStatus::Pending,
            attempts: 0,
            first_attempt_at: None,
            next_retry_at: None,
            locked_at: None,
            last_error: None,
            priority: 500,
            created_at: 0,
            supporting_commits: vec!["missing_sha_1234567890".to_string()],
        };

        // ensure_present must fail because supporting_commits contains a missing SHA
        assert!(worker.ensure_present(&row).await.is_err());

        // When supporting_commits is present locally, ensure_present must succeed
        let row_valid = FetchQueueRow {
            supporting_commits: vec![commit1.clone()],
            ..row
        };
        assert!(worker.ensure_present(&row_valid).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_oversized_git_range_rejected() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_path = temp_dir.path().to_path_buf();

        crate::git_cmd::in_dir_async(&repo_path)
            .arg("init")
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&repo_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;

        let tree_out = crate::git_cmd::in_dir_async(&repo_path)
            .arg("write-tree")
            .output()
            .await?;
        let tree = String::from_utf8(tree_out.stdout)?.trim().to_string();

        let base_out = crate::git_cmd::in_dir_async(&repo_path)
            .args(["commit-tree", &tree, "-m", "base"])
            .output()
            .await?;
        let base_sha = String::from_utf8(base_out.stdout)?.trim().to_string();

        let mut parent = base_sha.clone();
        for i in 0..=MAX_RANGE_COMMITS {
            let out = crate::git_cmd::in_dir_async(&repo_path)
                .args([
                    "commit-tree",
                    &tree,
                    "-p",
                    &parent,
                    "-m",
                    &format!("c{}", i),
                ])
                .output()
                .await?;
            parent = String::from_utf8(out.stdout)?.trim().to_string();
        }

        let worker = test_worker(repo_path).await;
        let row = FetchQueueRow {
            id: 1,
            patchset_id: None,
            cover_letter_message_id: None,
            repo_url: None,
            commit_hash: format!("{}..{}", base_sha, parent),
            mr_url: None,
            mr_title: None,
            mr_number: None,
            status: crate::db::FetchStatus::Pending,
            attempts: 0,
            first_attempt_at: None,
            next_retry_at: None,
            locked_at: None,
            last_error: None,
            priority: 500,
            created_at: 0,
            supporting_commits: Vec::new(),
        };

        let err = worker.ingest(&row).await.unwrap_err();
        assert!(
            err.downcast_ref::<PermanentFetchError>().is_some(),
            "expected PermanentFetchError, got: {:?}",
            err
        );
        assert!(err.to_string().contains("exceeding maximum of 256"));

        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_present_fetches_range_head_before_base() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let remote_path = temp_dir.path().join("remote");
        let local_path = temp_dir.path().join("local");
        std::fs::create_dir_all(&remote_path)?;
        std::fs::create_dir_all(&local_path)?;

        // Configure remote so only ref tips may be requested in `want` lines
        // (matching servers that disable allowReachableSHA1InWant or where
        // non-tip reachability walks time out).
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.name", "Test User"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "uploadpack.allowTipSHA1InWant", "true"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "uploadpack.allowReachableSHA1InWant", "false"])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["config", "uploadpack.allowAnySHA1InWant", "false"])
            .output()
            .await?;

        std::fs::write(remote_path.join("file.txt"), "base\n")?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["commit", "-m", "base commit"])
            .output()
            .await?;
        let base_sha = String::from_utf8(
            crate::git_cmd::in_dir_async(&remote_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        std::fs::write(remote_path.join("file.txt"), "head\n")?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["add", "."])
            .output()
            .await?;
        crate::git_cmd::in_dir_async(&remote_path)
            .args(["commit", "-m", "head commit"])
            .output()
            .await?;
        let head_sha = String::from_utf8(
            crate::git_cmd::in_dir_async(&remote_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .await?
                .stdout,
        )?
        .trim()
        .to_string();

        crate::git_cmd::in_dir_async(&local_path)
            .args(["init", "-b", "main"])
            .output()
            .await?;
        let worker = test_worker(local_path).await;

        let remote_url = format!("file://{}", remote_path.display());
        let row = FetchQueueRow {
            id: 1,
            patchset_id: None,
            cover_letter_message_id: None,
            repo_url: Some(remote_url),
            commit_hash: format!("{}..{}", base_sha, head_sha),
            mr_url: None,
            mr_title: None,
            mr_number: None,
            status: crate::db::FetchStatus::Fetching,
            attempts: 1,
            first_attempt_at: None,
            next_retry_at: None,
            locked_at: None,
            last_error: None,
            priority: 500,
            created_at: 0,
            supporting_commits: Vec::new(),
        };

        // Fetching head first brings base in the packfile without ever sending
        // `want <base_sha>` to the remote.
        worker.ensure_present(&row).await?;
        assert!(worker.is_present(&base_sha).await);
        assert!(worker.is_present(&head_sha).await);

        Ok(())
    }

    #[test]
    fn test_permanent_git_fetch_stderr_and_http_400_classification() {
        assert!(is_permanent_git_fetch_stderr(
            "fatal: remote error: upload-pack: not our ref c2f80695652667d6f1e1a5561af15948217d2315"
        ));
        assert!(is_permanent_git_fetch_stderr(
            "error: Server does not allow request for unadvertised object deadbeef"
        ));
        assert!(is_permanent_git_fetch_stderr(
            "fatal: couldn't find remote ref refs/heads/missing"
        ));
        assert!(!is_permanent_git_fetch_stderr(
            "error: RPC failed; HTTP 503 curl 22 The requested URL returned error: 503"
        ));

        assert!(is_http_400_packfile_rejection(
            "fetch of 2 object(s) failed: error: RPC failed; HTTP 400 curl 22 The requested URL returned error: 400\nfatal: expected 'packfile'"
        ));
        assert!(is_http_400_packfile_rejection(
            "fetch of 1 object(s) failed: error: RPC failed; HTTP 400 curl 22 The requested URL returned error: 400\nfatal: expected 'acknowledgments'"
        ));
        assert!(!is_http_400_packfile_rejection(
            "fetch of 1 object(s) failed: error: RPC failed; HTTP 500 curl 22 The requested URL returned error: 500\nfatal: expected 'packfile'"
        ));
    }

    #[tokio::test]
    async fn test_handle_failure_caps_http_400_retries_at_three_attempts() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let settings = crate::settings::DatabaseSettings {
            url: ":memory:".to_string(),
            token: String::new(),
        };
        let db = Arc::new(Database::new(&settings).await?);
        db.migrate().await?;
        let (tx, mut rx) = mpsc::channel(10);
        let worker = FetchWorker::new(temp_dir.path().to_path_buf(), Arc::clone(&db), tx, None);

        db.enqueue_fetch(
            None,
            Some("cover-400"),
            Some("https://example.com/repo.git"),
            "base..head",
            None,
            None,
            None,
            500,
        )
        .await?;

        let http_400_msg = "fetch of 2 object(s) failed: error: RPC failed; HTTP 400 curl 22 The requested URL returned error: 400\nfatal: expected 'packfile'";

        // Attempts 1 and 2 reschedule with backoff rather than failing terminally.
        for expected_attempt in 1..=2 {
            let mut row = db.lock_pending_fetch().await?.unwrap();
            assert_eq!(row.attempts, expected_attempt);
            worker.handle_failure(&row, anyhow!(http_400_msg)).await;
            assert!(rx.try_recv().is_err());
            // Fast-forward retry window so we can claim the next attempt.
            row.next_retry_at = Some(1);
            db.set_fetch_retry_at(row.id, 1, http_400_msg).await?;
        }

        // Attempt 3 hits MAX_HTTP_400_ATTEMPTS and fails terminally.
        let row3 = db.lock_pending_fetch().await?.unwrap();
        assert_eq!(row3.attempts, 3);
        worker.handle_failure(&row3, anyhow!(http_400_msg)).await;

        let ev = rx
            .try_recv()
            .expect("expected IngestionFailed on attempt 3");
        match ev {
            Event::IngestionFailed {
                article_id, error, ..
            } => {
                assert_eq!(article_id, "base..head");
                assert!(error.contains("HTTP 400"));
            }
            other => panic!("unexpected event: {:?}", other),
        }
        assert!(db.lock_pending_fetch().await?.is_none());

        Ok(())
    }
}
