FROM eclipse-temurin:21.0.7_6-jre-alpine-3.21
USER root
WORKDIR /opt/kafka/MM2LagExporter
RUN apk add bash
RUN adduser --uid 10101 -S kafka
RUN chown -R 10101 /opt/kafka/MM2LagExporter
ADD target/mm2-lag-exporter*.jar /opt/kafka/MM2LagExporter/mm2-lag-exporter.jar
USER 10101
EXPOSE 8080
ENTRYPOINT ["java","-jar","/opt/kafka/MM2LagExporter/mm2-lag-exporter.jar"]
