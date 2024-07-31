# Use the bitnami/kafka as the base image
FROM bitnami/kafka:latest

# Switch to the root user to install packages
USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Switch back to the original non-root user (usually 1001 in Bitnami images)
USER 1001

# Create a directory for the virtual environment
WORKDIR /app

# Create a virtual environment
RUN python3 -m venv venv

# Activate the virtual environment and install dependencies
COPY requirements.txt /app/
RUN . venv/bin/activate && pip install --no-cache-dir -r requirements.txt

# Copy your Kafka producer script into the container
#COPY ./kafka_producer.py /app/kafka_producer.py

# Set the working directory
WORKDIR /app

# Run your Kafka producer script using the virtual environment's Python
CMD ["venv/bin/python", "/kafka/kafka_producer.py"]