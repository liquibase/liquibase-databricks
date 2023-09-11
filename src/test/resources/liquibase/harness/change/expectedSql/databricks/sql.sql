CREATE TABLE main.liquibase_harness_test_ds.sqltest (id INT) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
insert into sqltest (id) values (1)
insert into sqltest (id) values (2)
insert into sqltest (id) values (3)