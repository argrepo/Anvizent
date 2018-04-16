# Mock Data Extraction Service Controller Bundle

This bundles the [Data Extraction Service Controller ](../des.controller) with data source adapters into a 
consolidated Spring Boot application.

## Building

To build the project, use command: `mvn clean install` from the project folder or the parent folder. After a successful, the build jar 
will be available in the 'target' folder. 

## Running

Java 8 is required.

Maven: `mvn spring-boot:run -Dserver.port=<port> -Dmessaging.service=<uri>`

Java: `java -Dserver.port=<port> -Dmessaging.service=<uri> -jar <path to project jar>`

The default port is 8080. The default messaging service URI is amqp://localhost. To define messaging service URI put appropriate value into MESSAGING_SERVICE environment variable.

Data Extraction Service depends on ETL jobs, the path to the folder which contains the jobs is set via DES_HOME environment variable. 
