"""
Extract metadata from Iceberg catalog and ingest into embeddings service.
Uses only stdlib HTTP client to avoid extra deps inside Spark image.
"""

import json
from urllib import request as urlrequest, error as urlerror
import config
from pyspark.sql import SparkSession

EMBED_URL = f"http://embed-service:{config.EMBED_SERVICE_PORT}/ingest"


def spark_session():
    return (
        SparkSession.builder
        .appName("extract-metadata")
        .config("spark.sql.catalog.bank", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bank.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.bank.uri", config.NESSIE_URI)
        .config("spark.sql.catalog.bank.ref", config.NESSIE_BRANCH)
        .config("spark.sql.catalog.bank.warehouse", config.WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def collect_tables(spark):
    namespaces = [r.namespace for r in spark.sql("SHOW NAMESPACES IN bank").collect()]
    out = []
    for ns in namespaces:
        rows = spark.sql(f"SHOW TABLES IN bank.{ns}").collect()
        for r in rows:
            out.append((ns, r.tableName))
    return out


def describe_table(spark, namespace, table):
    cols = spark.sql(f"DESCRIBE bank.{namespace}.{table}").collect()
    tbl_props = spark.sql(f"DESCRIBE EXTENDED bank.{namespace}.{table}").collect()
    return cols, tbl_props


def make_items(namespace, table, cols, tbl_props):
    items = []

    # table-level description (heuristic)
    table_desc = None
    for r in tbl_props:
        if r.col_name and "Comment" in str(r.col_name):
            table_desc = r.data_type or r.comment
    if table_desc:
        items.append({
            "obj_type": "table",
            "catalog": "bank",
            "namespace": namespace,
            "table_name": table,
            "column_name": None,
            "data_type": None,
            "description": f"{namespace}.{table}: {table_desc}",
            "source_ref": "describe_extended",
        })

    # columns
    for r in cols:
        name = (r.col_name or "").strip()
        dtype = (r.data_type or "").strip()
        cmt = (r.comment or "").strip() if hasattr(r, "comment") else ""
        if not name or name.startswith("#"):
            continue
        desc = cmt if cmt else f"{name} sütunu, tipi: {dtype}"
        items.append({
            "obj_type": "column",
            "catalog": "bank",
            "namespace": namespace,
            "table_name": table,
            "column_name": name,
            "data_type": dtype,
            "description": desc,
            "source_ref": "describe",
        })
    return items


def post_items(items):
    if not items:
        return
    payload = json.dumps({"items": items}).encode("utf-8")
    req = urlrequest.Request(
        EMBED_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=120) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"Ingest failed with status {resp.status}")
        _ = resp.read()


def main():
    spark = spark_session()
    tables = collect_tables(spark)
    print(f"[INFO] Found {len(tables)} tables")

    batch = []
    BATCH_SIZE = 200
    for ns, tbl in tables:
        try:
            cols, props = describe_table(spark, ns, tbl)
            items = make_items(ns, tbl, cols, props)
            batch.extend(items)
            if len(batch) >= BATCH_SIZE:
                post_items(batch)
                print(f"[INFO] Ingested {len(batch)} items")
                batch = []
        except Exception as e:
            print(f"[WARN] {ns}.{tbl}: {e}")
    if batch:
        post_items(batch)
        print(f"[INFO] Ingested remaining {len(batch)} items")
    spark.stop()


if __name__ == "__main__":
    main()

