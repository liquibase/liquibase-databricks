--liquibase formatted sql
--changeset cody:2
INSERT INTO main.liquibase.test_yml (id, name_of_col, test_struct) VALUES ('1', 'this_col', named_struct("ID", "2", "name","cody"))