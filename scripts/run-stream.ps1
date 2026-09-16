docker compose run --rm spark `
  /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.13 `
  /opt/spark-app/streaming_job.py