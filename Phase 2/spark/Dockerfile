FROM bitnami/spark:latest

# Copy the Spark script
#COPY ./temperature_processor.py /app/temperature_processor.py

# Switch to the root user to install packages
USER root

RUN adduser --disabled-password --gecos '' --uid 1000 sparkuser

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Switch back to the original non-root user (usually 1001 in Bitnami images)
USER 1001


# Create a virtual environment
RUN python3 -m venv venv

# Activate the virtual environment and install dependencies
COPY requirements.txt /app/
RUN . venv/bin/activate && pip install --no-cache-dir -r /app/requirements.txt

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Set the working directory
WORKDIR /app

# Use created User to circumvent kerberos error
USER sparkuser


# Set the entry point to run the Spark script
ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", "--conf",  "spark.jars.ivy=/tmp", "/spark/temperature_processor.py"]

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.jars.ivy="/tmp" <myscript>.py