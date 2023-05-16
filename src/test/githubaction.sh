
## Set env variables

DATABRICKS_HOST = ${GithubSecret.host}
DATABRICKS_TOKEN = ${GithubSecret.dbx_token}
TEST_CATALOG = ${GithubSecret.test_catalog}
TEST_SCHEMA = ${GithubSecret.test_schema}
WORKSPACE_ID = ${GithubSecret.workspace_id} ## this is the workspace id, that is after the hostname url in the workspace inside the databricks env

## install terraform


##
cd src/test/terraform


## Run terraform
TF_VAR_DBX_HOST=$DATABRICKS_HOST TF_VAR_DBX_TOKEN=$DATABRICKS_TOKEN TF_VAR_TEST_CATALOG=$TEST_CATALOG TF_VAR_TEST_SCHEMA=$TEST_SCHEMA terraform init
TF_VAR_DBX_HOST=$DATABRICKS_HOST TF_VAR_DBX_TOKEN=$DATABRICKS_TOKEN TF_VAR_TEST_CATALOG=$TEST_CATALOG TF_VAR_TEST_SCHEMA=$TEST_SCHEMA terraform plan
TF_VAR_DBX_HOST=$DATABRICKS_HOST TF_VAR_DBX_TOKEN=$DATABRICKS_TOKEN TF_VAR_TEST_CATALOG=$TEST_CATALOG TF_VAR_TEST_SCHEMA=$TEST_SCHEMA terraform apply
yes

## Get terraform cluster id

CLUSTER_ID=$(terraform output -raw cluster_url)


## Build url from host, clusterid, connschema, conncatalog

## url must be built
## github core path must be from secret, but HTTP path (cluster id must be dynamic from terraform output)


URL_ID="jdbc:databricks://$DATABRICKS_HOST:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/$WORKSPACE_ID/$CLUSTER_ID;AuthMech=3;ConnCatalog=$TEST_CATALOG;ConnSchema=$TEST_SCHEMA;"

## got back up to test root folder
cd ../

export DBX_DATABASE=databricks
export DATABRICKS_TOKEN=$DATABRICKS_TOKEN
export DBX_URL=$URL_ID

sed -i -r s"/<database>/$DBX_DATABASE/" resources/harness-config.yml
sed -i -r s"/<dbx_token>/$DATABRICKS_TOKEN/" resources/harness-config.yml
sed -i -r s"%<url>%$DBX_URL%" resources/harness-config.yml



## Run test
mvn surefire:test -Dtest=FoundationalHarnessTestSuite -DconfigFile=resources/harness-config.yml test


## Destroy resources

TF_VAR_DBX_HOST=$DATABRICKS_HOST TF_VAR_DBX_TOKEN=$DATABRICKS_TOKEN TF_VAR_TEST_CATALOG=$TEST_CATALOG TF_VAR_TEST_SCHEMA=$TEST_SCHEMA terraform destroy
yes
