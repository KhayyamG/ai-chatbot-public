import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import config

app = FastAPI(title="Metadata Chatbot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

client = OpenAI(api_key=config.OPENAI_API_KEY)

def get_conn():
    return psycopg.connect(
        host=config.PG_HOST, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASS,
        port=config.PG_PORT,
    )


class Ask(BaseModel):
    question: str
    top_k: int = 12


@app.post("/ask")
def ask(body: Ask):
    # 1) Embed the question
    q_emb = client.embeddings.create(model=config.EMBEDDING_MODEL, input=body.question).data[0].embedding
    vec_literal = "[" + ",".join(f"{x:.8f}" for x in q_emb) + "]"

    # 2) Vector similarity in PGVector
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.obj_type, o.catalog, o.namespace, o.table_name, o.column_name, o.data_type, o.description
            FROM embeddings e
            JOIN object_texts o ON o.text_id = e.text_id
            ORDER BY e.embedding <-> %s::vector
            LIMIT %s
            """,
            (vec_literal, body.top_k),
        )
        rows = cur.fetchall()

    # 3) Build context (TABLEs first)
    items = []
    for r in rows:
        items.append({
            "obj_type": r[0],
            "catalog": r[1],
            "namespace": r[2],
            "table_name": r[3],
            "column_name": r[4],
            "data_type": r[5],
            "description": r[6],
        })
    items.sort(key=lambda x: 0 if x["obj_type"] == "table" else 1)

    def fq(i):
        base = f"{i.get('catalog')}.{i.get('namespace')}.{i.get('table_name')}"
        return base if i["obj_type"] == "table" else f"{base}.{i.get('column_name')}"

    ql = body.question.lower()
    prefer_tables = any(k in ql for k in ["cədvəl", "cedvel", "table", "cədvəli"])
    if prefer_tables:
        table_items = [i for i in items if i["obj_type"] == "table"]
        if table_items:
            items = table_items + [i for i in items if i["obj_type"] != "table"]

    context_lines = []
    for i in items:
        prefix = "TABLE" if i["obj_type"] == "table" else "COLUMN"
        dtype = f" ({i.get('data_type')})" if i.get('data_type') else ""
        context_lines.append(f"- {prefix}: {fq(i)}{dtype} — {i['description']}")
    context = "\n".join(context_lines)

    prompt = (
        f"Sual: {body.question}\n"
        "Aşağıdakı metadata əsasında qısa, dəqiq və konkret cavab ver.\n"
        "Yalnız verilən məlumatdan istifadə et. Cavabı Azərbaycan dilində yaz.\n"
        "Cavabda lazım gələrsə tam cədvəl adını (catalog.namespace.table) ver.\n"
        f"Metadata:\n{context}\n"
        "Cavab:"
    )

    resp = client.responses.create(model=config.LLM_MODEL, input=prompt)
    answer = getattr(resp, "output_text", None) or str(resp)
    return {"answer": answer, "references": items}


@app.get("/health")
def health():
    return {"status": "ok"}

