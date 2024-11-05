# Introduction

Apache Flink is a distributed stream processing engine. Flink started as a research project in the database systems and distributed systems research group at the Technische Universität Berlin. It was later open sourced and became an Apache project. Flink is also backed by great community support of users and contributors. Flink can be used to write large-scale, mission critical applications that process large amounts of data. It can also be used for batch processing if needed but the primary use case is stream processing.

Flink provides low latency and high throughput for processing data streams. It also provides exactly-once processing semantics which is a key feature for building mission critical applications. It completes with technologies like Apache Spark, Apache Storm, and Apache Samza.

## Batch Processing vs Stream Processing

Batch processing is the processing of data in chunks. In batch processing, data is collected over a period of time, say every hour or every day and then processed. Batch processing is used when the data is not time-sensitive and can be processed after a period of time. Traditional ETL (Extract Transform Load) applications used to use this approach to process data. The data was loaded into traditional data warehouses and then queried on a scheduled basis. Again, scheduling part can be handled in multiple different ways and similarly data warehouse can be of many different types. Traditional warehouses were built on Oracle, IBM company proprietary database solutions. However, nowadays the data warehouses are built on cloud or on open source technologies like Iceberg. Batch processing is used in applications like ETL, data warehousing, and reporting where the data is processed and used to report some key KPI metrics to the stakeholders for overall visibility into the system.

Stream processing is the processing of data in real-time or near real-time. Here, near-real time means micro batches with frameworks such as Spark which runs with micro batches of data. In stream processing, data is processed as it arrives. Stream processing is used when the data is time-sensitive and needs to be processed as soon as it arrives. Stream processing is used in applications like real-time analytics, fraud detection, and monitoring user events on an ecommerce to quickly promote them product or to offer discounts. Stream processing is also essential for fraud detection to avoid fraudulent transactions. 

## Features

- *Open Source*: Apache Flink is an open source project.
- Connectors: Flink has connectors for many systems such as Kafka, Kinesis, and Elasticsearch.
- Event time processing: Flink has support for event time processing.
- True streaming: Flink has true streaming semantics.
- Exactly once processing: Flink has support for exactly once processing.
- Speed: It provides very low latency with upto millisecond processing time.
- Fault tolerance: Flink provides fault tolerance with state checkpoints and recovery.
- Stateful processing: Flink provides support for stateful processing.
- Windowing: Flink provides support for windowing operations.