ALTER TABLE main.liquibase_harness_test_ds.posts ADD CONSTRAINT FK_POSTS_AUTHORS_TEST_1 FOREIGN KEY (author_id) REFERENCES main.liquibase_harness_test_ds.authors (id)
ALTER TABLE main.liquibase_harness_test_ds.posts ADD CONSTRAINT FK_POSTS_AUTHORS_TEST_2 FOREIGN KEY (id) REFERENCES main.liquibase_harness_test_ds.authors (id)