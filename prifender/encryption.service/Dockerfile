#docker build -t encryption.service
#docker run -it -p 8080:8080 --rm encryption.service
FROM openjdk:8-alpine

VOLUME /tmp
COPY target/*.jar /app.jar

ENV JAVA_OPTIONS -Xmx1024m
ENV SPRING_PROFILES_ACTIVE default
#ENV SPRING_OPTIONS -Dspring.profiles.active=default

EXPOSE 8080

ENTRYPOINT java $JAVA_OPTIONS -jar $SPRING_OPTIONS /app.jar