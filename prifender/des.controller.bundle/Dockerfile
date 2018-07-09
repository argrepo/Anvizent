FROM store/oracle/serverjre:8

RUN mkdir /des

COPY target/*.jar /des/des.jar

ADD target/jobs /des/ETLJOBS

EXPOSE 8080

CMD java -jar /des/des.jar
