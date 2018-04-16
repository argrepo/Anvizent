FROM openjdk:8-jre

ARG JAR_FILE

ENV JAR=${JAR_FILE}

ADD target/${JAR_FILE} ${JAR_FILE}

ENTRYPOINT ["/bin/sh", "-c", "java -Dserver.port=8181 -Dmessaging.service=amqp://rabbit -jar ${JAR}"]