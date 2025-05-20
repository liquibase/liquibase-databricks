ALTER TABLE main.liquibase_harness_test_ds.authors ADD COLUMN stringColumn STRING
ALTER TABLE main.liquibase_harness_test_ds.authors ADD COLUMN varcharColumn VARCHAR(50)
ALTER TABLE main.liquibase_harness_test_ds.authors ADD COLUMN intColumn INT
ALTER TABLE main.liquibase_harness_test_ds.authors ADD COLUMN dateColumn date
UPDATE main.liquibase_harness_test_ds.authors SET stringColumn = 'INITIAL_VALUE'
UPDATE main.liquibase_harness_test_ds.authors SET varcharColumn = 'INITIAL_VALUE'
UPDATE main.liquibase_harness_test_ds.authors SET intColumn = 5
UPDATE main.liquibase_harness_test_ds.authors SET dateColumn = '2023-09-21'
ALTER TABLE main.liquibase_harness_test_ds.authors ALTER COLUMN stringColumn COMMENT 'Quite complicated remarks with "some text" double quoted and \'another\' single quoted'