ALTER TABLE main.liquibase_harness_test_ds.posts ADD COLUMN varcharColumn VARCHAR(25)
UPDATE main.liquibase_harness_test_ds.posts SET varcharColumn = 'INITIAL_VALUE'
ALTER TABLE main.liquibase_harness_test_ds.posts DROP COLUMN varcharColumn