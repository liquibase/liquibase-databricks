



# Liquibase-Databricks Connector


## Summary 
This is the repository for the Open Source version of the Liquibase Extension for Managing Delta Tables on DatabricksSQL.
The official documentation for this extension is here: [Using Liquibase with Databricks Platforms](https://docs.liquibase.com/start/tutorials/databricks/home.html)

Be aware that the official version of the documentation covers both Open Source as well as Liquibase Pro functionality for Databricks as well as how to use general Liquibase Pro features with Databricks. Liquibase Pro users should NOT use the Open Source version of the extension, but instead follow the instructions here: [Using Liquibase with Databricks SQL to set up the Pro version of the extension](https://docs.liquibase.com/start/tutorials/databricks/databricks.html).

If you are curious about Liquibase Pro, you can find more information here: [Liquibase Pro | Update database schema quickly & reliably](https://www.liquibase.com/liquibase-pro)


## NOTE! ONLY TABLES CREATED WITH UNITY CATALOG ARE SUPPORTED FOR MOST ADVANCED OPERATIONS
This extension utilizes Unity Catalog System tables for many advanced operations such as snapshotting, identifying various constraints (PK/FK/NOT NULL, etc.)
If hive_metastore is used, this is not tested and may not provide all the below functionality.


## Harness Status: 

1. [x] Add unit tests with liquibase test harness - Cody Davis - DONE
2. [x] Pass Foundational Test Harness - Cody Davis - DONE 4/1/2023
3. [x] Pass Contributed Test Harness - Cody Davis - DONE 9/15/2023
4. [x] Pass Advanced Test Harness - Cody Davis - DONE 9/28/2023


## Currently Supported Change Types:

### Contributed / Base
1. [x] createTable/dropTable 
2. [x] addColumn/dropColumn
3. [x] addPrimaryKey/dropPrimaryKey
4. [x] addForeignKey/dropForeignKey
5. [x] addNotNullConstraint/dropNotNullConstraint
6. [x] createTable/createTableDataTypeText/createTableTimestamp/dropTable
7. [x] createView/dropView
8. [x] dropAllForeignKeyConstraints
9. [x] createView/dropView
10. [x] setTableRemarks - supported but not returned in snapshot as JDBC Driver not populating it
11. [x] setColumnRemarks
12. [x] setViewRemarks (set in TBLPROPERTIES ('comment' = '<comment>'))
13. [x] executeCommand
14. [x] mergeColumns
15. [x] modifySql
16. [x] renameColumn
17. [x] renameView
18. [x] sql
19. [x] sqlFile
20. [x] Change Data Test: apply delete
21. [x] Change Data Test: apply insert
22. [x] Change Data Test: apply loadData
23. [x] Change Data Test: apply loadDataUpdate
24. [x] Add/Drop Check Constraints - supported but not returned in snapshot

### Advanced
1. [x] addColumn snapshot
2. [x] addPrimaryKey snapshot
3. [x] addForeignKey snapshot
4. [x] schemaAndCatalogSnapshot snapshot
5. [x] createTable snapshot
6. [x] createView snapshot
7. [x] generateChangelog -
8. [x] addUniqueConstraint -  not supported
9. [x] createIndex - Not Supported, use changeClusterColumns change type for datbricks to Map to CLUSTER BY ALTER TABLE statements for Delta Tables
10. [x] alterTableProperties
11. [x] alterCluster

### Databricks Specific:
1. [x] OPTIMIZE - optimizeTable - optimize with zorderCols options - <b> SUPPORTED </b> in Contributed Harness
2. [x] CLUSTER BY (DDL) - createClusteredTable - createTable with clusterColumns as additional option for liquid - <b> SUPPORTED </b> in Contributed Harness
3. [x] ANALYZE TABLE - analyzeTable - change type with compute stats column options - <b> SUPPORTED </b> in Contributed Harness
4. [x] VACUUM - vacuumTable - change type with retentionHours parameter (default is 168) - <b> SUPPORTED </b> in Contributed Harness
5. [x] ALTER CLUSTER KEY - alterCluster - change type that will be used until index change types are mapped with CLUSTER BY columns for snapshot purposes


## Remaining Required Change Types to Finish in Base/Contributed
1. [ ] (nice to have, not required) createFunction/dropFunction - in Liquibase Pro, should work in Databricks, but change type not accessible from Liquibase Core
2. [x] (nice to have, not required) addCheckConstraint/dropCheckConstraint - in Liquibase Pro, should work in Databricks, but change type not accessible from Liquibase Core
3. [ ] addDefaultValue (of various types). Databricks/Delta tables support this, but does not get populated by databricks in the JDBC Driver (COLUMN_DEF property always None even with default)
The remaining other change types are not relevant to Databricks and have been marked with INVALID TEST


## General TO DO:
1. [ ] Add support for Snapshotting complex types like STRUCT/MAP
2. [ ] Add support for snapshotting IDENTITY KEYs
3. [ ] Add TIMESTAMP_NTZ Data Type

## Aspirational Roadmap - Databricks Specific Additional Change Types to Add: 

1. COPY INTO
2. MERGE
3. RESTORE VERSION AS OF
4. ANALYZE TABLE - Code Complete - Adding Tests - Cody Davis
5. CLONE
6. BLOOM FILTERS - Maybe do not support, CLUSTER BY should be the primary indexing mechanism long term
7. OPTIMIZE / ZORDER - Code Complete - Adding Tests - Cody Davis
8. VACUUM - Code Complete - Adding Tests - Cody Davis
9. SYNC IDENTITY
10. VOLUMES
11. GRANT / REVOKE statements


## How to use the Liquibase-Databricks Extension

### Steps: 

1. Download and install liquibase from [here](https://docs.liquibase.com/start/install/home.html)

2. Download the Databricks Driver from [here](https://www.databricks.com/spark/jdbc-drivers-download). 
Then put this driver jar under the liquibase/lib directory. 

3. Build this project or retrieve the jar from the latest release. 
Then put this extension jar under the liquibase/lib directory. 

4. IMPORTANT: If using Linux/MaxOS - run the following command in your terminal before continuing (you can add this to the bash/zsh profile):
   export JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED

5. Edit the connection parameters to your Databricks catlaog/database under the liquibase.properties file. The format will look like this:

```
url: jdbc:databricks://<workspace_url>:443/default;transportMode=http;ssl=1;httpPath=<http_path>;AuthMech=3;ConnCatalog=<catalog>;ConnSchema=<database>; 
username: token
password: <dbx_token>
```

Where the following parameters are: 
<li> <b>workspace_url</b>: The url of the host name you are connecting to</li>
<li> <b>dbx_token</b>: The token of your user or application service principal authorized for running any needed Liquibase operations.</li>
<li> <b>http_path</b>: This is the http_path of a Databricks SQL Warehouse or a Databricks Cluster (DBR). Either cluster type can be used. Best Results are with Serverless SQL Warehouses. </li>
<li> <b>catalog</b>: The catalog name you want to connect to (default is main). </li>
<li> <b>database</b>: The database / schema name you want to connect to. </li>



5. Add changes and run your change logs like so:
```
   liquibase --changeLogFile=changelog.sql update
```

