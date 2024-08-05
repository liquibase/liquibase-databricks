CREATE TABLE main.liquibase_harness_test_ds.test_table_alter_cluster (test_id INT NOT NULL, test_new INT, CONSTRAINT PK_TEST_TABLE_ALTER_CLUSTER PRIMARY KEY (test_id)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = true)
ALTER TABLE main.liquibase_harness_test_ds.test_table_alter_cluster CLUSTER BY (test_id)
ALTER TABLE main.liquibase_harness_test_ds.test_table_alter_cluster CLUSTER BY (test_id,test_new)
