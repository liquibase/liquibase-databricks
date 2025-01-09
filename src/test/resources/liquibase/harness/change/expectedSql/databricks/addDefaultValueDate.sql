ALTER TABLE main.liquibase_harness_test_ds.authors ADD COLUMN dateTimeColumn TIMESTAMP
ALTER TABLE main.liquibase_harness_test_ds.authors ALTER COLUMN dateTimeColumn SET DEFAULT '2008-02-12 12:34:03'