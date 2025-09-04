import psycopg
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from openai import OpenAI
import config

app = FastAPI(title="Embed Service")

client = OpenAI(api_key=config.OPENAI_API_KEY)

def get_conn():
    return psycopg.connect(
        host=config.PG_HOST, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASS,
        port=config.PG_PORT
    )

class IngestItem(BaseModel):
    obj_type: str
    catalog: Optional[str] = None
    namespace: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    data_type: Optional[str] = None
    description: str
    source_ref: Optional[str] = None

class IngestPayload(BaseModel):
    items: List[IngestItem]

@app.post("/ingest")
def ingest(payload: IngestPayload):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for it in payload.items:
                cur.execute(
                    """
                    INSERT INTO object_texts(obj_type,catalog,namespace,table_name,column_name,data_type,description,source_ref)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING text_id
                    """,
                    (it.obj_type, it.catalog, it.namespace, it.table_name, it.column_name, it.data_type, it.description, it.source_ref)
                )
                text_id = cur.fetchone()[0]

                emb = client.embeddings.create(model=config.EMBEDDING_MODEL, input=it.description).data[0].embedding
                dims = len(emb)
                cur.execute(
                    """
                    INSERT INTO embeddings(text_id, model, dims, embedding)
                    VALUES (%s,%s,%s,%s)
                    """,
                    (text_id, config.EMBEDDING_MODEL, dims, emb)
                )
    return {"ok": True, "count": len(payload.items)}

@app.get("/health")
def health():
    return {"status": "ok"}

