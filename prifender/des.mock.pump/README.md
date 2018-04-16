# Mock Data Pump

To run, use the following command:

java -Ddatabase.url="[url]" -Ddatabase.namespace=[namespace] -jar [pump]

* **database.url:** Full JDBC URL for the where to pump mock data. Make sure to include username and password.
* **database.namespace:** The prefix to use when naming tables. Typically database and/or schema.
* **mock.des.url (optional):** The URL of the Mock Data Extraction Service that will be used to generate the data. Defaults to http://localhost:8080/des.
* **mock.data.size (optional):** The size of the mock data source. Defaults to 10,000.
* **mock.data.minimal (optional):** Controls whether the PayChecks table is generated. Since PayChecks table is 10 times the length of the Employees table, this option should be turned on when generating very large data sets. Defaults to false.
* **perf.parallel.processes (optional):** Controls how many parallel processes are used when pumping mock data. Defaults to 10.
* **perf.batch.size (optional):** Controls the number of SQL insert operations that are batched into one operation. Defaults to 1000.

# Optimal Insert Strategy Probe

A tool for profiling the performance of different insert strategies for a database.

To run, use the following command:

java -Ddatabase.url="[url]" -Ddatabase.namespace=[namespace] -jar [pump] -probeOptimalInsertStrategy

* **database.url:** Full JDBC URL for the where to pump mock data. Make sure to include username and password.
* **database.namespace:** The prefix to use when naming tables. Typically database and/or schema.
* **perf.batch.size (optional):** Controls the number of SQL insert operations that are batched into one operation. Defaults to 1000.

# Optimal Batch Size Probe

A tool for finding an optimal batch size for a given insert strategy on a given database.

To run, use the following command:

java -Ddatabase.url="[url]" -Ddatabase.namespace=[namespace] -jar [pump] -probeOptimalBatchSize [insert-strategy]

* **insert-strategy:** Either PreparedStatementBatching, RegularStatementBatchingA or RegularStatementBatchingA.
* **database.url:** Full JDBC URL for the where to pump mock data. Make sure to include username and password.
* **database.namespace:** The prefix to use when naming tables. Typically database and/or schema.
* **perf.probe.optimalBatchSize.batchSizes:** The list of batch sizes to profile, separated by a comma.

# JDBC URL Patterns

* MySQL: jdbc:mysql://[user]:[password]@[host]:[port]/[database]
* Oracle : jdbc:oracle://[user]:[password]@[host]:[port]/[database]
* PostgreSQL: jdbc:postgresql://[host]:[port]/[database]?user=[user]&password=[password]
* SQL Server: jdbc:sqlserver://[host]:[port];databaseName=[database];user=[user];password=[password]
