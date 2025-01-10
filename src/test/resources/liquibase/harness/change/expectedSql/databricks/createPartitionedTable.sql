CREATE TABLE main.liquibase_harness_test_ds.test_table_partitioned (test_id INT NOT NULL, test_column VARCHAR(50) NOT NULL, partition_column STRING NOT NULL, CONSTRAINT PK_TEST_TABLE_PARTITIONED PRIMARY KEY (test_id)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = true) PARTITIONED BY (partition_column)
CREATE TABLE main.liquibase_harness_test_ds.partitioned_delta_table (id INT, name VARCHAR(20), some_column BIGINT) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = true, 'this.is.my.key' = 12, 'this.is.my.key2' = true) LOCATION 's3://databricks-th/partitioned_delta_table' PARTITIONED BY (id, some_column)