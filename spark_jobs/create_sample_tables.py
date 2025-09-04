# spark_jobs/create_sample_tables.py
from pyspark.sql import SparkSession
import config

# Spark + Iceberg + Nessie connection
spark = (
    SparkSession.builder
    .appName("create-sample-tables")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS bank.gold")

# Drop if exist to ensure clean metadata in demos
for t in [
    "bank.gold.card_transaction",
    "bank.gold.loan_schedule",
    "bank.gold.payment",
    "bank.gold.customer_loan",
    "bank.gold.customer",
]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(
    """
CREATE TABLE bank.gold.customer (
  customer_id BIGINT COMMENT 'Müştərinin unikal ID-si',
  full_name   STRING COMMENT 'Ad və soyad',
  birth_date  DATE   COMMENT 'Doğum tarixi',
  segment     STRING COMMENT 'Müştəri seqmenti (retail/SME/corporate)',
  created_at  TIMESTAMP COMMENT 'Yaradılma tarixi'
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
COMMENT 'Müştəri referans cədvəli'
"""
)

spark.sql(
    """
CREATE TABLE bank.gold.customer_loan (
  loan_id         BIGINT  COMMENT 'Kreditin unikal ID-si',
  customer_id     BIGINT  COMMENT 'Müştəri ID-si (customer.customer_id)',
  contract_number STRING  COMMENT 'Kredit müqaviləsinin nömrəsi',
  balance_amount  DECIMAL(18,2) COMMENT 'Cari kredit qalığı',
  currency        STRING  COMMENT 'Valyuta (AZN/USD və s.)',
  opened_date     DATE    COMMENT 'Kreditin açıldığı tarix',
  status          STRING  COMMENT 'Status (active/closed/overdue)'
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
COMMENT 'Müştərinin kreditləri haqqında məlumat'
"""
)

spark.sql(
    """
CREATE TABLE bank.gold.payment (
  payment_id   BIGINT          COMMENT 'Ödənişin unikal ID-si',
  loan_id      BIGINT          COMMENT 'Aid olduğu kreditin ID-si',
  payment_date DATE            COMMENT 'Ödəniş tarixi',
  amount       DECIMAL(18,2)   COMMENT 'Ödəniş məbləği',
  method       STRING          COMMENT 'Metod (card/cash/transfer)',
  source_system STRING         COMMENT 'Məlumatın gəldiyi sistem'
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
COMMENT 'Kredit ödənişləri'
"""
)

spark.sql(
    """
CREATE TABLE bank.gold.loan_schedule (
  schedule_id    BIGINT         COMMENT 'Cədvəl sətrinin ID-si',
  loan_id        BIGINT         COMMENT 'Kreditin ID-si',
  installment_no INT            COMMENT 'Taksit nömrəsi (1..N)',
  due_date       DATE           COMMENT 'Planlaşdırılan ödəniş tarixi',
  principal_due  DECIMAL(18,2)  COMMENT 'Plan əsas məbləğ',
  interest_due   DECIMAL(18,2)  COMMENT 'Plan faiz məbləği',
  paid_flag      BOOLEAN        COMMENT 'Bu taksit ödənibmi?'
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
COMMENT 'Kreditlərin plan cədvəli — aylıq ödəniş məbləği = principal_due + interest_due'
"""
)

spark.sql(
    """
CREATE TABLE bank.gold.card_transaction (
  txn_id         BIGINT         COMMENT 'Tranzaksiyanın unikal ID-si',
  customer_id    BIGINT         COMMENT 'Müştəri ID-si',
  card_pan_mask  STRING         COMMENT 'Maskalanmış kart nömrəsi (xxxx-xxxx-xxxx-1234)',
  txn_amount     DECIMAL(18,2)  COMMENT 'Tranzaksiya məbləği',
  txn_currency   STRING         COMMENT 'Valyuta',
  txn_type       STRING         COMMENT 'Növ (purchase/refund/atm və s.)',
  txn_ts         TIMESTAMP      COMMENT 'Tranzaksiya vaxtı',
  mcc_code       STRING         COMMENT 'Merchant category code'
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
COMMENT 'Kartla edilən tranzaksiyalar'
"""
)

print("=== Tables in bank.gold ===")
spark.sql("SHOW TABLES IN bank.gold").show(truncate=False)

spark.stop()

