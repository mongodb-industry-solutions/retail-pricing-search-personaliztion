# Kafka Sink to MongoDB 

This repository contains Kafka connector configuration files which enables real time data synchronization from Kafka to MongoDB.

## Prerequisites

- Kafka Cluster
- MongoDB Instance

## Setup 

- Download & Install the MongoDB Source & Kafka Sink Connector Plugin in your Kafka Cluster 
    - https://www.confluent.io/hub/mongodb/kafka-connect-mongodb

- Update the following in the `mongodb-source-connector.properties` connector configuration file.
    - `CONNECTION-STRING` - MongoDB Cluster Connection String
    - `DB-NAME` - Database Name
    - `COLLECTION-NAME` - Collection Name

- Update the following in the `*-sink-connector.properties` connector configuration file.
    - `TOPIC-NAME` - Kafka Topic Name (i.e DB.COLLECTION name)

- Deploy the connector configuration files in your Kafka Cluster. This will enable real time data synchronization from Kafka to MongoDB.

**Note**: The above connector push the data to the Mongodb Collection at a regular interval of time from kafka, these configuration can be modified based on the use case. Refer to the following documentation for more details.
- [MongoDB Sink Configuration](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/)
