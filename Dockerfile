FROM apache/airflow:2.8.2

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get install -y libgomp1

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt .
RUN pip install -r requirements.txt

# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f

# Requirement for running Docker Operator
RUN pip install apache-airflow-providers-docker==3.7.5

# Requirement for running SparkSubmitOperator
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3

RUN airflow db upgrade