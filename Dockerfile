# Base Airflow image with Python 3.12
FROM apache/airflow:2.9.3-python3.12

USER root

# Install Java 17 for Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (amd64 or arm64 if you run on m-chips Macbooks)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Copy requirements.txt and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt