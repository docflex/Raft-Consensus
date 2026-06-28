FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY target/raft-consensus-1.0-SNAPSHOT.jar app.jar

EXPOSE 8080 9001

ENTRYPOINT ["java", "-jar", "app.jar"]
