CREATE TABLE IF NOT EXISTS {{ params.dds_schema_name }}".failed_entities (
    id SERIAL PRIMARY KEY,
    entity_name VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    entity JSONB,
    found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
