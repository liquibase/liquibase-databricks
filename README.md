# liquibase-databricks


## Current Summary 
Base and Foundational Change types should be supported at this stage. Change types such as procedures, triggers, merge column, indexes are not supported. 
Databricks specific change types that are added are listed below along with their completion status. 


## To Do: 

1. Add unit tests with liquibase test harness - Cody Davis - Done
2. Pass Foundational Test Harness - Cody Davis - Done
3. Pass Contributed Test Harness - Cody Davis - In Progress - ETA May 15, 2023
4. Pass Advanced Test Harness - Unassigned - Not Started

## Change Types to Add: 

1. COPY INTO
2. MERGE
3. RESTORE VERSION AS OF
4. ANALYZE TABLE - Code Complete - Cody Davis
5. SET TBL PROPERTIES - In Progress - Cody Davis (Defaults are in createTable change type with min required table props to support Liquibase)
6. CLONE
7. BLOOM FILTERS
8. OPTIMIZE / ZORDER - Code Complete - No Test Yet - Cody Davis
9. VACUUM - Code Complete - Cody Davis
10. SYNC IDENTITY - Not Started -



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

