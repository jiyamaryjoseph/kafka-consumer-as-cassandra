# Confluent Kafka Python Data Pipeline
![alt text](image-2.png)

This repository provides a solution to publish and consume data to and from Apache Kafka using the Confluent Kafka Python client, with messages formatted in JSON.

## Introduction

The Confluent Kafka Python Data Pipeline project offers a streamlined approach to working with Apache Kafka, a distributed event streaming platform, in Python. It facilitates the publishing and consumption of JSON-formatted data to and from Kafka clusters.

## Setup

Follow these steps to set up the environment and run the application:

1. **Create Conda Environment**:

conda create -n myenv python=3.8 -y


2. **Activate Environment**:

conda activate myenv

3. **Install Requirements**:

pip install -r requirements.txt


4. **Set Environment Variables**:
Update the necessary environment variables in the `.env` file with your Kafka cluster credentials, schema registry API credentials, and DATASTAX Astra Cassandra.

5. **Build Docker Image**:

docker build -t data-pipeline:lts .


6. **Run Docker Container**:
For Linux or macOS:

docker run -it -v $(pwd)/logs:/logs --env-file=$(pwd)/.env data-pipeline:lts


## Environment Variables

- **Cluster Environment Variables**:
- `API_KEY`: API key for accessing the Kafka cluster.
- `API_SECRET_KEY`: Secret key for accessing the Kafka cluster.
- `BOOTSTRAP_SERVER`: Kafka bootstrap server address.

- **Schema Registry Environment Variables**:
- `SCHEMA_REGISTRY_API_KEY`: API key for accessing the Schema Registry.
- `SCHEMA_REGISTRY_API_SECRET`: Secret key for accessing the Schema Registry.
- `ENDPOINT_SCHEMA_URL`: URL of the Schema Registry endpoint.

- **Database Environment Variable**:
- `CASSENDRA_CONNECT_BUNDLE`: Bundle download from Datastax astra
- `CASSENDRA_TOKEN_FILE`: token download from Datastax astra

## Docker Usage

This project utilizes Docker for containerization. Follow the instructions provided in the "Setup" section above to build the Docker image and run the container.



## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.




