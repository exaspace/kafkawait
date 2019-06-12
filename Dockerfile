FROM openjdk:12.0.1-jdk

USER daemon

COPY build/libs/*jar /kafkawait/

COPY docker/run_class.sh /run_class.sh

WORKDIR /kafkawait
