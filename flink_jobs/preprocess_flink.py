# Flink batch preprocessing script using Table API.
# Notes:
# - Requires pyflink installed and a running Flink cluster (start via Cygwin ./bin/start-cluster.sh on Windows).
# - If pyflink isn't available in your environment, use the Spark or Pandas fallback in spark_jobs/ and spark_jobs/pandas_fallback.py.
from pyflink.table import EnvironmentSettings, TableEnvironment, expressions as E
from pyflink.table.expressions import lit
import os

def main():
    source_path = os.path.abspath('data/raw_customer_data.csv').replace('\\', '/')
    source_uri = 'file:///' + source_path

    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Create a file system source table (CSV)
    t_env.execute_sql(f"""
    CREATE TABLE raw_data (
      customer_id STRING,
      age INT,
      salary DOUBLE,
      num_transactions INT,
      last_login STRING,
      region STRING
    ) WITH (
      'connector' = 'filesystem',
      'path' = '{source_uri}',
      'format' = 'csv'
    )
    """)

    table = t_env.from_path('raw_data')

    # 1. Remove rows with null customer_id
    table = table.filter(E.col('customer_id').is_not_null())

    # 2. Replace null age/salary with defaults using COALESCE
    table = table.add_columns(
        E.call_sql('COALESCE(age, 30)').alias('age_filled'),
        E.call_sql('COALESCE(salary, 50000)').alias('salary_filled')
    )

    # 3. Drop duplicates (distinct)
    table = table.distinct()

    # 4. Feature engineering: avg_value_per_txn and salary_z (z-score approximation)
    table = table.add_columns(
        (E.col('salary_filled') / (E.col('num_transactions') + lit(1))).alias('avg_value_per_txn'),
        ((E.col('salary_filled') - lit(50000)) / lit(20000)).alias('salary_z')
    )

    # Create processed sink (parquet)
    out_path = os.path.abspath('data/processed').replace('\\', '/')
    out_uri = 'file:///' + out_path
    t_env.execute_sql(f"""
    CREATE TABLE processed_data (
      customer_id STRING,
      age_filled INT,
      salary_filled DOUBLE,
      num_transactions INT,
      last_login STRING,
      region STRING,
      avg_value_per_txn DOUBLE,
      salary_z DOUBLE
    ) WITH (
      'connector' = 'filesystem',
      'path' = '{out_uri}',
      'format' = 'parquet'
    )
    """)

    table.execute_insert('processed_data').wait()
    print('Flink preprocessing complete. Output saved to data/processed (parquet).')

if __name__ == '__main__':
    main()
