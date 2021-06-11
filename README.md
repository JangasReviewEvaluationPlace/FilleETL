# File ETL - Readme

It's a Python based repository for an ETL to proceed data from files,
unify format through different file sources and send them to an
Apache Kafka broker for getting proceed for different usecases.

## Requirements & Setup
There are different setup options:
1. Use pure Python & Python packages
2. Use Docker & Docker Compose

### Requirements
- Python 3.7 or higher
- poetry or pip (I do recommend poetry, not only for this project but
    also in general)
- Docker & Docker Compose (optional)
- Running Kafka Cluster & Kafka Connect (Optional - just required if
    the generalized data should be proceed into a database)

## Workflow
ETL Flow will happens in two steps:
1. Load Source data, bring them into a specific CSV format
2. Consume that CSV with a Kafka Connector for getting automatically
    proceed into the kafka topic

The files wont get proceed directly into the broker.
That may allows to have some general format changes at a later point
of that project without rerunning the whole ETL process which might
take some time to proceed. Another use case could be that the data
simply needs to get reproceeded at some point and we might not run
the whole ETL process again.
