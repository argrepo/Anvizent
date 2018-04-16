# Mock Data Extraction Service

This service implements [Data Extraction Service (DES) API](https://github.com/Prifender/prifender/blob/master/des.api/swagger.yaml) and simulates the
behavior of a real DES.

* Implements two data source types: Mock Relational Database and Mock Hierarchical Database
* Connecting to either of the mock databases requires just the user name and the password
* The user name must be 'joe'
* The password must be 'somebody'
* The mock databases present the standard sample schema and data

Limitations:

* Connection profiles and running jobs are not persisted
* Pausing and resuming a data extraction job is not supported
* Progress of the data extraction job is not calculated
* Data sampling is always the first X number of rows (not randomized)
* Data transformation post extraction is not supported

## Building

To build the project, use command: `mvn clean install` from the project folder or the parent folder. After a successful, the build jar 
will be available in the 'target' folder. 

## Running

Java 8 is required.

Maven: `mvn spring-boot:run -Dserver.port=<port> -Dmessaging.service=<uri>`

Java: `java -Dserver.port=<port> -Dmessaging.service=<uri> -jar <path to project jar>`

The default port is 8080. The default messaging service URI is amqp://localhost.

## Sample Usage of the Mock Relational Database

Define a data source.

POST to /des/dataSources

    {
        "label": "Mock Relational DB",
        "description": "My mock description",
        "type": "relational.database.mock",
        "connectionParams":
        [
            {
                "id": "user",
                "value": "joe"
            },
            {
                "id": "password",
                "value": "somebody"
            }
        ]
    }
    
Run a data extraction job. After starting the job, monitor the messaging service for the extracted data.

POST to /des/dataExtractionJobs

    {
        "dataSource": "Mock_Relational_DB",
        "collection": "Employees",
        "scope": "all",
        "attributes":
        [
            {
                "name": "id"
            },
            {
                "name": "first_name"
            },
            {
                "name": "last_name"
            }
        ]
    }

## Sample Usage of the Mock Hierarchical Database

Define a data source.

POST to /des/dataSources

    {
        "label": "Mock Hierarchical DB",
        "description": "My mock description",
        "type": "hierarchical.database.mock",
        "connectionParams": [
            {
                "id": "user",
                "value": "joe"
            },
            {
                "id": "password",
                "value": "somebody"
            }
        ]
    }
    
Run a data extraction job. After starting the job, monitor the messaging service for the extracted data.

POST to /des/dataExtractionJobs

    {
        "dataSource": "Mock_Hierarchical_DB",
        "collection": "Employees",
        "scope": "sample",
        "sampleSize": "100",
        "attributes":
        [
            {
                "name": "first_name"
            },
            {
                "name": "last_name"
            },
            {
                "name": "phoneNumber"
            },
            {
                "name": "address",
                "children":
                [
                    {
                        "name": "city"
                    },
                    {
                        "name": "state"
                    }
                ]
            }
        ]
    }
