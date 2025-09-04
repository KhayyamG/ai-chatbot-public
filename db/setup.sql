CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS object_texts (
  text_id BIGSERIAL PRIMARY KEY,
  obj_type TEXT NOT NULL,   -- 'table' | 'column'
  catalog  TEXT,
  namespace TEXT,
  table_name TEXT,
  column_name TEXT,
  data_type TEXT,
  description TEXT,
  source_ref TEXT
);

CREATE TABLE IF NOT EXISTS embeddings (
  text_id BIGINT PRIMARY KEY REFERENCES object_texts(text_id) ON DELETE CASCADE,
  model   TEXT NOT NULL,
  dims    INT  NOT NULL,
  embedding VECTOR(3072) NOT NULL
);

CREATE INDEX IF NOT EXISTS embeddings_idx
  ON embeddings USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);

