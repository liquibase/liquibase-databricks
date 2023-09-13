# liquibase-databricks


## Summary 
This is the Liquibase Extension for Managing Delta Tables with DatabricksSQL. 

Base/Contributed and Foundational Change types should be supported at this stage. Change types such as procedures, triggers, sequences, indexes are not supported. 
Databricks specific change types that are added are listed below along with their completion status. 
Databricks tables creates with liquibase are automatically created with the Delta configs / versions that are required for all passing change types including: 'delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name'


## NOTE! ONLY TABLES CREATED WITH UNITY CATALOG ARE SUPPORTED FOR MOST ADVANCED OPERATIONS
This extension utilizes Unity Catalog System tables for many advanced operations such as snapshotting, identifying various constraints (PK/FK/NOT NULL, etc.)
If hive_metastore is used, this is not tested and may not provide all the below functionality.


## TO DO: 

1. Add unit tests with liquibase test harness - Cody Davis - DONE
2. Pass Foundational Test Harness - Cody Davis - DONE 4/1/2023
3. Pass Contributed Test Harness - Cody Davis - IN PROGRESS 9/15/2023
4. Pass Advanced Test Harness - Cody Davis - IN PROGRESS


## Currently Supported Change Types:
1. createTable/dropTable
2. addColumn/dropColumn
3. addPrimaryKey/dropPrimaryKey
4. addForeignKey/dropForeignKey
5. addNotNullConstraint/dropNotNullConstraint
6. createTable/createTableDataTypeText/createTableTimestamp/dropTable
7. createView/dropView
8. dropAllForeignKeyConstraints
9. createView/dropView
10. setTableRemarks
11. setColumnRemarks
12. setViewRemarks (set in TBLPROPERTIES ('comment' = '<comment>'))
13. executeCommand
14. mergeColumns
15. modifySql
16. renameColumn
17. renameView
18. sql
19. sqlFile
20. Change Data Test: apply delete
21. Change Data Test: apply insert
22. Change Data Test: apply loadData


## Remaining Required Change Types to Finish in Base/Contributed
1. createFunction/dropFunction - in Liquibase Pro, should work in Databricks, but change type not accessible from Liquibase Core
2. addCheckConstraint/dropCheckConstraint - in Liquibase Pro, should work in Databricks, but change type not accessible from Liquibase Core
3. addLookupTable (executing out of order/dropping FK before creation)
4. Change Data Test: apply loadUpdateData


The remaining other change types are not relevant to Databricks and have been marked with INVALID TEST


## Aspirational Roadmap - Databricks Specific Additional Change Types to Add: 

1. COPY INTO
2. MERGE
3. RESTORE VERSION AS OF
4. ANALYZE TABLE - Code Complete - Cody Davis
5. SET TBL PROPERTIES - In Progress - Cody Davis (Defaults are in createTable change type with min required table props to support Liquibase)
6. CLONE
7. BLOOM FILTERS
8. OPTIMIZE / ZORDER - Code Complete - No Test Yet - Cody Davis
9. VACUUM - Code Complete - No Test Yet - Cody Davis
10. SYNC IDENTITY
11. VOLUMES
12. GRANT / REVOKE statements



## How to use the Liquibase-Databricks Extension

### Steps: 

1. Download and install liquibase from [here](https://docs.liquibase.com/start/install/home.html)

2. Download the Databricks Driver from [here](https://www.databricks.com/spark/jdbc-drivers-download). 
Then put this driver jar under the liquibase/lib directory. 

3. Build this project or retrieve the jar from the latest release. 
Then put this extension jar under the liquibase/lib directory. 

4. Edit the connection parameters to your Databricks catlaog/database under the liquibase.properties file. The format will look like this:

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

