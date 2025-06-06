FROM openjdk:11-jdk-slim

# Install pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Config environment
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PYSPARK_PYTHON=python3

# install python packages
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PySpark
COPY ./tar/pyspark-3.5.5.tar.gz /tar/
RUN pip install /tar/pyspark-3.5.5.tar.gz

# Copy source code
COPY ./core ./core

CMD ["python3", "core/streaming/run_streaming.py"]
