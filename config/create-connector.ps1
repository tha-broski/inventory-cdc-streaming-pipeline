curl.exe -X PUT `
  -H "Content-Type: application/json" `
  --data-binary "@config\debezium-postgres-source-config.json" `
  http://localhost:8083/connectors/inventory-postgres-source/config