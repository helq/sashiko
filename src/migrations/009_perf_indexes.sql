CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON reviews(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_findings_review_severity ON findings(review_id, severity, preexisting);
CREATE INDEX IF NOT EXISTS idx_patches_message_id ON patches(message_id);
