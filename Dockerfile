FROM apache/airflow:2.10.3

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

USER airflow
# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64/"

RUN pip install pyspark
RUN pip install yadisk
RUN pip install playwright 
RUN playwright install chromium-headless-shell
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

USER root

RUN playwright install-deps