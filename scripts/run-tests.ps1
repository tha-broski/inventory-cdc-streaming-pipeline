docker compose run --rm `
  -e PYTHONPATH=/opt/spark-app `
  spark `
  python3 -m pytest /opt/spark-tests -v `
  -p no:cacheprovider