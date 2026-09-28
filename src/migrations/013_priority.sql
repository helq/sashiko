-- Priority columns and index for patchset priority queueing.
--
-- Adds priority, base_priority, and priority_cap columns to patchsets table,
-- and creates an index on (status, priority DESC, date ASC) for efficient
-- priority-ordered queue retrieval.
--
-- Applied atomically during application startup within a single SQLite
-- transaction before workers spawn. SQLite guarantees full transactional DDL
-- atomicity for both ALTER TABLE and CREATE INDEX statements.

ALTER TABLE patchsets ADD COLUMN base_priority INTEGER;
ALTER TABLE patchsets ADD COLUMN priority_cap INTEGER;
ALTER TABLE patchsets ADD COLUMN priority INTEGER DEFAULT 500;

CREATE INDEX IF NOT EXISTS idx_patchsets_status_priority_date
    ON patchsets(status, priority DESC, date ASC);
