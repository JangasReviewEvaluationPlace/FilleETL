curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "$(<'sftp_source_connector.json')"