"""
# Fabric Metadata & Data Quality Assessment Framework (CI/CD Ready)

## 1. Overview & Architecture
This framework provides an automated, end-to-end data quality assessment for Microsoft Fabric Lakehouses. 
It extracts structural metadata, profiles columns, evaluates custom quality rules, generates an aggregate 
quality index, persists execution metrics to a Delta audit table, and evaluates deployment gates for DataOps/CI/CD pipelines.

### Platform Placement
             Microsoft Fabric
                    │
                 OneLake
                    │
                 Lakehouse
                    │
             ┌──────┴──────┐
             │             │
          Metadata      Dataset
             │             │
             └──────┬──────┘
                    │
             Data Profiling
                    │
          Data Quality Rules
                    │
             Quality Score
                    │
       ┌────────────┴────────────┐
       │                         │
Delta Audit Log        CI/CD Deployment Gate
 (OneLake Delta)        (Exit Status/Payload)
"""

# ==============================================================================
# 2. Configuration, Parameterization & Environment Selection
# ==============================================================================
import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnull, sum as _sum, avg,
    min as _min, max as _max, lit, rand, expr, countDistinct, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

spark = SparkSession.builder.getOrCreate()

# Fetch parameters from Fabric Data Factory or default to local interactive values
try:
    dbutils.widgets.text("ENVIRONMENT", "DEV", "Target Environment")
    dbutils.widgets.text("TARGET_TABLE", "demo_synthetic_retail", "Target Table Name")
    dbutils.widgets.text("USE_SYNTHETIC_DATA", "True", "Execution Mode")
    dbutils.widgets.text("PASS_THRESHOLD", "85.0", "Quality Index Threshold")
    dbutils.widgets.text("PIPELINE_RUN_ID", "local_run", "Orchestrator Run ID")

    ENV = dbutils.widgets.get("ENVIRONMENT").upper()
    TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")
    USE_SYNTHETIC_DATA = dbutils.widgets.get("USE_SYNTHETIC_DATA").lower() == "true"
    PASS_THRESHOLD = float(dbutils.widgets.get("PASS_THRESHOLD"))
    PIPELINE_RUN_ID = dbutils.widgets.get("PIPELINE_RUN_ID")
except Exception:
    ENV = "DEV"
    TARGET_TABLE = "demo_synthetic_retail"
    USE_SYNTHETIC_DATA = True
    PASS_THRESHOLD = 85.0
    PIPELINE_RUN_ID = "local_manual_run"

SYNTHETIC_ROW_COUNT = 10000

ENV_CONFIG = {
    "DEV": {"strict_gate": False, "audit_table": "dev_dq_audit_log"},
    "TEST": {"strict_gate": True,  "audit_table": "test_dq_audit_log"},
    "PROD": {"strict_gate": True,  "audit_table": "prod_dq_audit_log"}
}
current_env_cfg = ENV_CONFIG.get(ENV, ENV_CONFIG["DEV"])

print(f"=== [CI/CD DataOps Gate] Running DQ Engine in '{ENV}' Environment ===")
print(f"Mode: {'Synthetic Data' if USE_SYNTHETIC_DATA else f'Lakehouse Table ({TARGET_TABLE})'}")
print(f"Pass Threshold: {PASS_THRESHOLD}% | Strict Gate: {current_env_cfg['strict_gate']}")

# ==============================================================================
# 3. Data Ingestion / Synthetic Data Generation
# ==============================================================================
if USE_SYNTHETIC_DATA:
    df_base = spark.range(0, SYNTHETIC_ROW_COUNT).select(
        when(rand() < 0.014, (col("id") % 100).cast("integer")).otherwise(col("id").cast("integer")).alias("order_id"),
        when(rand() < 0.002, lit(None)).otherwise((col("id") % 1000 + 100).cast("string")).alias("customer_id"),
        (col("id") % 500 + 1).cast("integer").alias("product_id"),
        (expr("abs(cast(rand() * 5 as int)) + 1")).alias("quantity"),
        when(rand() < 0.003, -10.0).otherwise(expr("round(rand() * 100 + 10, 2)")).alias("unit_price"),
        expr("current_timestamp()").alias("transaction_timestamp")
    )
    df_target = df_base
    table_name = "demo_synthetic_retail"
else:
    df_target = spark.table(TARGET_TABLE)
    table_name = TARGET_TABLE

df_target.cache()
total_rows = df_target.count()
print(f"Target Dataset '{table_name}' loaded successfully. Total Rows: {total_rows:,}")

# ==============================================================================
# 4. Metadata Discovery
# ==============================================================================
metadata_schema = []
for field in df_target.schema.fields:
    metadata_schema.append({
        "Column Name": field.name,
        "Data Type": field.dataType.simpleString(),
        "Nullable": field.nullable
    })

df_metadata = spark.createDataFrame(metadata_schema)
print(f"=== METADATA PROFILE: {table_name} ===")
df_metadata.show(truncate=False)

# ==============================================================================
# 5. Statistical Data Profiling
# ==============================================================================
profile_exprs = []
for c in df_target.columns:
    profile_exprs.extend([
        count(when(col(c).isNull(), 1)).alias(f"{c}__null_cnt"),
        countDistinct(c).alias(f"{c}__distinct_cnt"),
        _min(c).cast("string").alias(f"{c}__min_val"),
        _max(c).cast("string").alias(f"{c}__max_val")
    ])

profile_row = df_target.select(profile_exprs).collect()[0]

profiling_results = []
for c in df_target.columns:
    null_cnt = profile_row[f"{c}__null_cnt"]
    profiling_results.append({
        "Column": c,
        "Null Count": null_cnt,
        "Null Percentage": round((null_cnt / total_rows) * 100, 2),
        "Distinct Values": profile_row[f"{c}__distinct_cnt"],
        "Min Value": str(profile_row[f"{c}__min_val"]),
        "Max Value": str(profile_row[f"{c}__max_val"])
    })

df_profile = spark.createDataFrame(profiling_results)
print("=== DATA PROFILING SUMMARY ===")
df_profile.show(truncate=False)

# ==============================================================================
# 6. Data Quality Rules Execution
# ==============================================================================
dq_checks = []

# Check 1: Completeness - Customer ID Nulls
null_cust_cnt = df_target.filter(col("customer_id").isNull()).count()
null_cust_pct = round((null_cust_cnt / total_rows) * 100, 2)
dq_checks.append({
    "CheckName": "Null customer IDs",
    "Dimension": "Completeness",
    "MetricValue": f"{null_cust_pct}%",
    "Status": "PASS" if null_cust_pct <= 0.5 else "FAIL",
    "Weight": 20,
    "Passed": 1 if null_cust_pct <= 0.5 else 0
})

# Check 2: Uniqueness - Order ID Duplicates
distinct_orders = df_target.select("order_id").distinct().count()
dup_order_pct = round(((total_rows - distinct_orders) / total_rows) * 100, 2)
dq_checks.append({
    "CheckName": "Duplicate orders",
    "Dimension": "Uniqueness",
    "MetricValue": f"{dup_order_pct}%",
    "Status": "WARNING" if 0.5 < dup_order_pct <= 2.0 else ("PASS" if dup_order_pct <= 0.5 else "FAIL"),
    "Weight": 25,
    "Passed": 0.75 if 0.5 < dup_order_pct <= 2.0 else (1 if dup_order_pct <= 0.5 else 0)
})

# Check 3: Validity - Quantity > 0
invalid_qty_cnt = df_target.filter(col("quantity") <= 0).count()
invalid_qty_pct = round((invalid_qty_cnt / total_rows) * 100, 2)
dq_checks.append({
    "CheckName": "Invalid quantities",
    "Dimension": "Validity",
    "MetricValue": f"{invalid_qty_pct}%",
    "Status": "PASS" if invalid_qty_pct == 0.0 else "FAIL",
    "Weight": 25,
    "Passed": 1 if invalid_qty_pct == 0.0 else 0
})

# Check 4: Validity - Unit Price > 0
invalid_price_cnt = df_target.filter(col("unit_price") <= 0).count()
invalid_price_pct = round((invalid_price_cnt / total_rows) * 100, 2)
dq_checks.append({
    "CheckName": "Invalid sales amount",
    "Dimension": "Validity",
    "MetricValue": f"{invalid_price_pct}%",
    "Status": "PASS" if invalid_price_pct <= 0.5 else "FAIL",
    "Weight": 30,
    "Passed": 1 if invalid_price_pct <= 0.5 else 0
})

df_dq_results = spark.createDataFrame(dq_checks)
print("=== RULE EVALUATION METRICS ===")
df_dq_results.select("CheckName", "Dimension", "MetricValue", "Status").show(truncate=False)

# ==============================================================================
# 7. Data Quality Index (DQI) Calculation
# ==============================================================================
total_weight = sum([r["Weight"] for r in dq_checks])
weighted_passed = sum([r["Weight"] * r["Passed"] for r in dq_checks])
overall_dq_score = round((weighted_passed / total_weight) * 100, 1)

print("=" * 45)
print(f"  OVERALL DATA QUALITY SCORE: {overall_dq_score}%")
print("=" * 45)

# ==============================================================================
# 8. Automated Remediation Recommendations
# ==============================================================================
print("=== AUTOMATED REMEDIATION RECOMMENDATIONS ===")
for r in dq_checks:
    if r["Status"] == "WARNING":
        print(f"- [WARNING] {r['CheckName']}: Metric is {r['MetricValue']}. Apply `dropDuplicates(['order_id'])` downstream.")
    elif r["Status"] == "FAIL":
        print(f"- [ACTION REQUIRED] {r['CheckName']}: Metric is {r['MetricValue']}. Investigate source pipeline.")

# ==============================================================================
# 9. Audit Logging & Result Persistence (OneLake Delta)
# ==============================================================================
audit_payload = [{
    "pipeline_run_id": PIPELINE_RUN_ID,
    "environment": ENV,
    "target_table": table_name,
    "overall_dq_score": overall_dq_score,
    "checks_detail": json.dumps(dq_checks),
    "execution_timestamp": datetime.datetime.utcnow().isoformat()
}]

df_audit = spark.createDataFrame(audit_payload)
df_audit.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(current_env_cfg["audit_table"])

print(f"[DataOps] Results successfully logged to Delta audit table: '{current_env_cfg['audit_table']}'")

# ==============================================================================
# 10. CI/CD Deployment Gate & Exit Evaluation
# ==============================================================================
gate_passed = overall_dq_score >= PASS_THRESHOLD

gate_output = {
    "status": "SUCCESS" if gate_passed else "FAILED",
    "dqi_score": overall_dq_score,
    "threshold": PASS_THRESHOLD,
    "environment": ENV,
    "target_table": table_name,
    "pipeline_run_id": PIPELINE_RUN_ID
}

print("\n" + "=" * 50)
print(f" CI/CD DEPLOYMENT GATE STATUS: {gate_output['status']}")
print("=" * 50)

if not gate_passed:
    failure_msg = f"Deployment Gate Blocked! DQI Score ({overall_dq_score}%) < Threshold ({PASS_THRESHOLD}%)."
    if current_env_cfg["strict_gate"]:
        print(f"[CI/CD GATE BLOCKED] {failure_msg}")
        try:
            mssparkutils.notebook.exit(json.dumps(gate_output))
        except NameError:
            pass
        raise ValueError(failure_msg)
    else:
        print(f"[WARNING] {failure_msg} (Skipping exit block because strict_gate=False in {ENV})")

try:
    mssparkutils.notebook.exit(json.dumps(gate_output))
except NameError:
    print("[INFO] Execution finished in interactive mode.")
