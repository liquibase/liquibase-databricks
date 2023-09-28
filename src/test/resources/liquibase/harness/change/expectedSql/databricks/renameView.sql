CREATE VIEW main.liquibase_harness_test_ds.test_view AS select id, first_name, last_name, email from authors
ALTER VIEW main.liquibase_harness_test_ds.test_view RENAME TO test_view_new