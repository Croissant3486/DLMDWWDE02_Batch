FROM python:latest

# Switch to the root user to install packages
USER root

RUN adduser --disabled-password --gecos '' --uid 1000 hadoop

# Create a virtual environment
RUN python -m venv /app/venv

# Install dependencies into the virtual environment
COPY requirements.txt /app/
RUN /app/venv/bin/pip install --no-cache-dir -r /app/requirements.txt

# Set environment variables for Spark to use the virtual environment
ENV PATH="/app/venv/bin:${PATH}"

# Set the working directory
WORKDIR /app

USER hadoop

CMD [ "python", "/visuals/visualize_data.py" ]
