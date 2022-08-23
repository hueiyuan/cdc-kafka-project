# cdc-kafka-project
The project which implement and integrate cdc sink and source connector of the Apache Kafka in docker container

## Folder Structure
```
|-- example/
    |-- delta_cdc_example.py
```

* `example/delta_cdc_example.py`: Using spark stuctureed streaming and delta lake merge into mechanism to achieve CDC (Change Data Capture) from kafka topic. And we can use kafka connector to produce mysql, mongodb data to kafka with debezium connector.
