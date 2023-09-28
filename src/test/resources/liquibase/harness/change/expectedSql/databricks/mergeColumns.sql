CREATE TABLE main.liquibase_harness_test_ds.full_name_table (first_name VARCHAR(50), last_name VARCHAR(50)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
INSERT INTO main.liquibase_harness_test_ds.full_name_table (first_name) VALUES ('John')
UPDATE main.liquibase_harness_test_ds.full_name_table SET last_name = 'Doe' WHERE first_name='John'
INSERT INTO main.liquibase_harness_test_ds.full_name_table (first_name) VALUES ('Jane')
UPDATE main.liquibase_harness_test_ds.full_name_table SET last_name = 'Doe' WHERE first_name='Jane'
ALTER TABLE main.liquibase_harness_test_ds.full_name_table ADD COLUMN full_name VARCHAR(255)
UPDATE main.liquibase_harness_test_ds.full_name_table SET full_name = first_name || ' ' || last_name
ALTER TABLE main.liquibase_harness_test_ds.full_name_table DROP COLUMN first_name
ALTER TABLE main.liquibase_harness_test_ds.full_name_table DROP COLUMN last_name