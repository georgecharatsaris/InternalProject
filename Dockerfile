FROM ubuntu:latest

RUN apt-get update \
    && apt-get upgrade \
    && apt -y install wget

RUN apt-get -y install openjdk-8-jdk-headless
RUN apt -y install python3
RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz

RUN tar -xzf spark-3.0.1-bin-hadoop2.7.tgz
RUN mv spark-3.0.1-bin-hadoop2.7 /opt/spark
RUN rm spark-3.0.1-bin-hadoop2.7.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
 
COPY /jars/*.jar /opt/spark/jars

WORKDIR /home/project

CMD ["tail", "-f", "/dev/null"]