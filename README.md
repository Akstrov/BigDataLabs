# Big Data Labs

This repository contains hands-on laboratory work focused on distributed systems and big data technologies. These labs are part of a course exploring various aspects of distributed computing and big data processing.

## Project Structure

- **lab0/**: Initial setup and environment configuration
  - Docker configuration for the distributed environment
  - Configuration files for Hadoop, HBase, Spark, and Kafka
  - Setup scripts for various services

- **lab2/**: HDFS Operations Lab
  - Basic HDFS file operations
  - File status checking
  - Read/Write operations in HDFS

- **lab3_mapreduce/**: MapReduce Programming
  - Implementation of WordCount example
  - Custom Mapper and Reducer implementations
  - Basic text processing with MapReduce

- **lab_kafka/**: Kafka producers, consumers, and Streams examples
  - Kafka producer and consumer examples
  - Kafka Streams word-count example
  - Interactive word producer/consumer

## Technologies Used

- **Apache Hadoop**: Distributed storage and processing
- **Apache HBase**: Distributed database
- **Apache Spark**: Fast data processing engine
- **Apache Kafka**: Distributed streaming platform
- **Docker**: Containerization and environment setup
- **Java**: Primary programming language
- **Maven**: Project management and build tool

## Prerequisites

- Docker and Docker Compose (for optional local setups)
- Java Development Kit (JDK)
- Maven
- Git

## Setup

Some labs provide Docker configuration and helper scripts for local service setup; see `lab0/` for those assets.

## Building

Each lab directory is a Maven project and can be built with `mvn clean package`.

## Lab Descriptions

### Lab 0: Environment Setup
- Basic environment configuration
- Docker container setup
- Service initialization

### Lab 2: HDFS Operations
- Learning HDFS architecture
- Implementing basic HDFS operations
- Understanding distributed file systems

### Lab 3: MapReduce Programming
- Word count implementation
- Understanding MapReduce paradigm
- Text processing in distributed environment

#### Lab 3 (Python Streaming example)

This lab additionally contains a Python streaming variant of the WordCount example. The streaming mapper/reducer scripts are in `lab3_mapreduce/mapper.py` and `lab3_mapreduce/reducer.py`.

### Lab Kafka

- Kafka producers, consumers, and a Kafka Streams word-count example.
- Key files are located in `lab_kafka/src/main/java/edu/ismagi/kafka/`.
- Demonstrates basic message production/consumption, interactive word streaming, and a streams-based word count.

## Notes

- Each lab builds upon concepts from previous labs
- Additional labs and technologies will be added as the course progresses
- Configuration files may need adjustments based on your local setup

## Contributing

This is a learning project developed as part of coursework. While it's primarily for educational purposes, suggestions and improvements are welcome.

## Future Additions

This repository is actively maintained and will be updated with:
- Additional labs covering more distributed systems concepts
- Enhanced documentation and examples
- New technology integrations
- Performance optimization examples

---
*This is an educational project focused on learning distributed systems and big data technologies.*