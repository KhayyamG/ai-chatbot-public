import os

# === MinIO / Iceberg / Nessie ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
NESSIE_BRANCH = os.getenv("NESSIE_BRANCH", "main")

# === PostgreSQL / PGVector ===
PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "ai_chatbot")
PG_USER = os.getenv("PGUSER", "ai")
PG_PASS = os.getenv("PGPASSWORD", "ai_pass")

# === OpenAI ===
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")

# === Services ===
EMBED_SERVICE_PORT = int(os.getenv("EMBED_SERVICE_PORT", "7000"))
CHATBOT_API_PORT = int(os.getenv("CHATBOT_API_PORT", "8000"))

# Alias
WAREHOUSE = WAREHOUSE_PATH.rstrip("/")

