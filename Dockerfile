FROM eclipse-temurin:17-ubi10-minimal

COPY build/libs/*jar /kafkawait/

COPY docker/run_class.sh /run_class.sh

WORKDIR /kafkawait
