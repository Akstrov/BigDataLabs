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

## Technologies Used

- **Apache Hadoop**: Distributed storage and processing
- **Apache HBase**: Distributed database
- **Apache Spark**: Fast data processing engine
- **Apache Kafka**: Distributed streaming platform
- **Docker**: Containerization and environment setup
- **Java**: Primary programming language
- **Maven**: Project management and build tool

## Prerequisites

- Docker and Docker Compose
- Java Development Kit (JDK)
- Maven
- Git

## Setup Instructions

1. Clone the repository
2. Navigate to the `lab0` directory
3. Run Docker Compose to set up the environment:
   ```bash
   docker-compose up -d
   ```

## Building and Running Projects

Each lab directory contains its own Maven project. To build a specific lab:

```bash
cd lab<number>
mvn clean package
```

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

In addition to the Java MapReduce examples, this lab includes a Python streaming implementation of the WordCount example. The Python mapper and reducer are available in `lab3_mapreduce/mapper.py` and `lab3_mapreduce/reducer.py` respectively.

Notes:
- Make sure the `mapper.py` and `reducer.py` files are accessible from the Hadoop nodes (for example by placing them in a shared volume or HDFS where the streaming job can read them).
- Ensure `mapper.py` is executable and that `python3` is available on the nodes that run the map and reduce tasks.

Example Hadoop streaming command (as used in the course):

```bash
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar \
  -files /shared_volume/mapper.py,/shared_volume/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input ./alice.txt \
  -output ./python_word_count
```

Replace `/shared_volume/mapper.py` and `/shared_volume/reducer.py` with the actual paths where you placed the files on your system or container. The `-files` argument makes the mapper and reducer scripts available to the tasks.

The example above will read input from `./alice.txt` and write the result to `./python_word_count` in HDFS (or the working directory depending on your cluster configuration).

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