FROM eclipse-temurin:17.0.10_7-jre-alpine
USER root
WORKDIR /opt/kafka/MM2LagExporter
RUN apk add bash
RUN adduser --uid 10101 -S kafka
RUN chown -R 10101 /opt/kafka/MM2LagExporter
ADD target/mm2-lag-exporter*.jar /opt/kafka/MM2LagExporter/mm2-lag-exporter.jar
USER 10101
EXPOSE 8080
ENTRYPOINT ["java","-jar","/opt/kafka/MM2LagExporter/mm2-lag-exporter.jar"]
