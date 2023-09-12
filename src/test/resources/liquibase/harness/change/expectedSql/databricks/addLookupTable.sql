CREATE TABLE main.liquibase_harness_test_ds.authors_data AS SELECT DISTINCT email AS authors_email FROM main.liquibase_harness_test_ds.authors WHERE email IS NOT NULL
ALTER TABLE main.liquibase_harness_test_ds.authors_data ALTER COLUMN  authors_email SET NOT NULL
ALTER TABLE main.liquibase_harness_test_ds.authors_data ADD CONSTRAINT pk_authors_email PRIMARY KEY (authors_email)
ALTER TABLE main.liquibase_harness_test_ds.authors ADD CONSTRAINT FK_AUTHORS_AUTHORS_DATA FOREIGN KEY (email) REFERENCES main.liquibase_harness_test_ds.authors_data (authors_email)
