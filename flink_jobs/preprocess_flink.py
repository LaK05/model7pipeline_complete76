from pyflink.table import EnvironmentSettings, TableEnvironment

# ============================================================
# INITIALIZE ENVIRONMENT
# ============================================================
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# Drop previous tables to prevent conflicts
t_env.execute_sql("DROP TABLE IF EXISTS raw_data")
t_env.execute_sql("DROP TABLE IF EXISTS processed_data")

# ============================================================
# DEFINE CSV SOURCE — ACCURATE SCHEMA (23 columns)
# ============================================================
t_env.execute_sql("""
CREATE TABLE raw_data (
  status STRING,
  card_present_flag INT,
  bpay_biller_code STRING,
  account STRING,
  currency STRING,
  long_lat STRING,
  txn_description STRING,
  merchant_id STRING,
  merchant_code STRING,
  first_name STRING,
  balance DOUBLE,
  txn_date STRING,             -- renamed from "date" to avoid reserved keyword issue
  gender STRING,
  age DOUBLE,
  merchant_suburb STRING,
  merchant_state STRING,
  extraction STRING,
  amount DOUBLE,
  transaction_id STRING,
  country STRING,
  customer_id STRING,
  merchant_long_lat STRING,
  movement STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///C:/model7pipeline/data/raw/raw_customer_data.csv',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
""")

# ============================================================
# STEP 1 — LOAD RAW DATA
# ============================================================
table = t_env.from_path("raw_data")

# ============================================================
# STEP 2 — HANDLE MISSING VALUES (COALESCE)
# ============================================================
t_env.create_temporary_view(
    "filled_data",
    t_env.sql_query("""
        SELECT
          status,
          COALESCE(card_present_flag, 0) AS card_present_flag,
          COALESCE(bpay_biller_code, '') AS bpay_biller_code,
          account,
          currency,
          long_lat,
          txn_description,
          merchant_id,
          COALESCE(merchant_code, '') AS merchant_code,
          first_name,
          COALESCE(balance, 0.0) AS balance,
          txn_date,
          gender,
          COALESCE(age, 35) AS age,
          merchant_suburb,
          merchant_state,
          extraction,
          COALESCE(amount, 0.0) AS amount,
          transaction_id,
          country,
          customer_id,
          merchant_long_lat,
          movement
        FROM raw_data
    """)
)

# ============================================================
# STEP 3 — REMOVE DUPLICATES
# ============================================================
t_env.create_temporary_view(
    "deduped_data",
    t_env.sql_query("SELECT DISTINCT * FROM filled_data")
)

# ============================================================
# STEP 4 — FEATURE ENGINEERING
# Add normalized_balance and transaction_density
# ============================================================
t_env.create_temporary_view(
    "final_data",
    t_env.sql_query("""
        SELECT
          *,
          (balance / (amount + 1)) AS avg_transaction_value,
          ((balance - 500) / 200) AS normalized_balance
        FROM deduped_data
    """)
)

# ============================================================
# STEP 5 — DEFINE OUTPUT TABLE (FILESYSTEM CSV SINK)
# ============================================================
t_env.execute_sql("""
CREATE TABLE processed_data (
  status STRING,
  card_present_flag INT,
  bpay_biller_code STRING,
  account STRING,
  currency STRING,
  long_lat STRING,
  txn_description STRING,
  merchant_id STRING,
  merchant_code STRING,
  first_name STRING,
  balance DOUBLE,
  txn_date STRING,
  gender STRING,
  age DOUBLE,
  merchant_suburb STRING,
  merchant_state STRING,
  extraction STRING,
  amount DOUBLE,
  transaction_id STRING,
  country STRING,
  customer_id STRING,
  merchant_long_lat STRING,
  movement STRING,
  avg_transaction_value DOUBLE,
  normalized_balance DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///C:/model7pipeline/data/processed/cleaned_output.csv',
  'format' = 'csv'
)
""")

# ============================================================
# STEP 6 — EXECUTE PIPELINE
# ============================================================
t_env.execute_sql("INSERT INTO processed_data SELECT * FROM final_data").wait()

print("Preprocessing completed successfully.")
print("Output saved to: C:/model7pipeline/data/processed/cleaned_output.csv/")
