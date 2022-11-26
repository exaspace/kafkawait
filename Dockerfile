FROM eclipse-temurin:18

COPY build/libs/*jar /kafkawait/

COPY docker/run_class.sh /run_class.sh

WORKDIR /kafkawait
