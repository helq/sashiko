-- Migration 014: Add arrival timestamp for batch deprioritization
ALTER TABLE patchsets ADD COLUMN created_at INTEGER;
CREATE INDEX IF NOT EXISTS idx_patchsets_arrival ON patchsets(COALESCE(created_at, date), baseline_id);
