FROM bitnami/spark:latest

# Copy the Spark script
#COPY ./temperature_processor.py /app/temperature_processor.py

# Switch to the root user to install packages
USER root

RUN adduser --disabled-password --gecos '' --uid 1000 hadoop

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create a virtual environment
RUN python3 -m venv /app/venv

# Install dependencies into the virtual environment
COPY requirements.txt /app/
RUN /app/venv/bin/pip install --no-cache-dir -r /app/requirements.txt

# Set environment variables for Spark to use the virtual environment
ENV PATH="/app/venv/bin:${PATH}"

# Set the working directory
WORKDIR /app

USER hadoop

# Set the entry point to run the Spark script
ENTRYPOINT ["spark-submit",  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", "--conf",  "spark.jars.ivy=/tmp", "/spark/temperature_processor.py"]

#"--master", "spark://spark-master:7077",