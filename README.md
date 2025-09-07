![AI Chatbot Architecture](https://github.com/KhayyamG/ai-chatbot-public/blob/15fe8b3aa105ceb719e27d3cc46a8307db024b60/ai_chatbot.jpg)
# AI Chatbot (Public)

A minimal RAG-style metadata chatbot powered by:
- Spark + Iceberg + Nessie (metadata store on MinIO)
- PostgreSQL + pgvector (embeddings store)
- FastAPI services for embedding and Q&A

## Prerequisites
- Docker Desktop (Compose v2)
- Internet access to pull images
- OpenAI API key

## Quick Start
1. Copy `.env.example` to `.env` and set your key:
   - `OPENAI_API_KEY=sk-...`
2. Build and start:
   - `docker compose up -d --build`
3. Ensure MinIO bucket is ready (auto-created):
   - Console: http://localhost:9001 (minio/minio12345)
4. Create sample Iceberg tables and ingest metadata:
   - `docker compose exec -it spark bash`
   - `spark-submit /opt/spark_jobs/create_sample_tables.py`
   - `spark-submit /opt/spark_jobs/extract_metadata.py`
   - `exit`
5. Ask the chatbot:
   - Swagger: http://localhost:8000/docs (POST `/ask`)
   - Example body:
     `{ "question": "kreditlərin plan cədvəli hansıdır?", "top_k": 12 }`

## Services
- MinIO: 9000/9001
- Nessie: 19120
- Postgres: 5432 (db `ai_chatbot`, user `ai`, pass `ai_pass`)
- Adminer: 8080
- Embed Service: 7000
- Chatbot API: 8000

## Notes
- No secrets are committed; `.env` is ignored by `.gitignore`.
- Spark image bundles Iceberg/Nessie/S3 JARs for offline runs.
- Data lives in Docker volumes `minio_data`, `pg_data` (ignored).


