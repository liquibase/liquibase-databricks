CREATE TABLE main.liquibase_harness_test_ds.test_table_clustered (test_id INT NOT NULL, test_column VARCHAR(50) NOT NULL, CONSTRAINT PK_TEST_TABLE_CLUSTERED PRIMARY KEY (test_id)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name') CLUSTER BY (test_id)