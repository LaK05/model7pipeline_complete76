model7pipeline_complete
=======================

Objective
---------
End-to-end project that implements the four assessment components:
1. Data preprocessing using Apache Flink (or fallback to Spark/Pandas)
2. Real-time streaming with Kafka + producer/consumer + ML inference
3. Incremental model updates (partial_fit) using streaming data (CDC simulation)
4. In-memory analytics using Spark or Pandas fallback

Structure
---------
- docker/                  : Docker Compose (Kafka + Zookeeper)
- flink_jobs/              : PyFlink preprocessing job (batch)
- kafka_clients/           : Kafka producer, consumer (inference + model update), CDC simulator
- models/                  : training & model files (train and saved models)
- spark_jobs/              : Spark in-memory job and Pandas fallback
- docs/                    : Execution steps and notes for Windows + Cygwin
- data/                    : Contains your uploaded dataset raw_customer_data.csv (if provided)
- requirements.txt         : Python dependencies
- README.md

Important
---------
This project is prepared for Windows with Cygwin for Flink shell scripts. Some components (PyFlink, PySpark)
require matching Python and Java versions. See docs/execution_steps.txt for detailed setup and troubleshooting.

