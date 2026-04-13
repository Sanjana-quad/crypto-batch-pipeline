FROM apache/airflow:2.8.1

# Step 1: root for system installs
USER root

# Java (for Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk wget

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Step 2: install Hadoop client
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /opt/hadoop && \
    rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

USER airflow
# Step 3: install python deps
COPY requirements.txt /tmp/requirements.txt
RUN /usr/local/bin/python -m pip install --no-cache-dir -r /tmp/requirements.txt

# Step 4: switch to airflow user (FINAL)
