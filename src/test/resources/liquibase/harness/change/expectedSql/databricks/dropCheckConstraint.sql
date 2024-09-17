ALTER TABLE main.liquibase_harness_test_ds.posts ADD CONSTRAINT test_check_constraint CHECK (id > 0)
ALTER TABLE main.liquibase_harness_test_ds.posts DROP CONSTRAINT test_check_constraint