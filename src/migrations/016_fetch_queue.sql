CREATE TABLE IF NOT EXISTS fetch_queue (
    id INTEGER PRIMARY KEY,
    patchset_id INTEGER,
    cover_letter_message_id TEXT,
    repo_url TEXT NOT NULL DEFAULT '',
    commit_hash TEXT NOT NULL DEFAULT '',
    commit_range TEXT NOT NULL DEFAULT '',
    article_id TEXT,
    mr_url TEXT,
    mr_title TEXT,
    mr_number INTEGER,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    first_attempt_at INTEGER,
    next_retry_at INTEGER,
    locked_at INTEGER,
    last_error TEXT,
    error_log TEXT,
    priority INTEGER DEFAULT 500,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(patchset_id) REFERENCES patchsets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fetch_queue_status
    ON fetch_queue(status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_fetch_queue_patchset_id
    ON fetch_queue(patchset_id);

CREATE INDEX IF NOT EXISTS idx_fetch_queue_article_id
    ON fetch_queue(article_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fetch_queue_repo_article
    ON fetch_queue(repo_url, article_id);

CREATE INDEX IF NOT EXISTS idx_fetch_queue_claim
    ON fetch_queue(status, attempts ASC, priority DESC, created_at ASC, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_fetch_queue_commit ON fetch_queue(commit_hash);

CREATE TABLE IF NOT EXISTS fetch_supporting_commits (
    id INTEGER PRIMARY KEY,
    fetch_id INTEGER NOT NULL,
    commit_hash TEXT NOT NULL,
    FOREIGN KEY(fetch_id) REFERENCES fetch_queue(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fetch_supporting_fetch_id
    ON fetch_supporting_commits(fetch_id);
