-- liquibase formatted sql

CREATE TABLE snapshot_test_table (snapshot_test_column INT(10)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');

CREATE TABLE test_table (id INT(10) NOT NULL) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');

CREATE TABLE test_table_xml (test_column INT NULL) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');

CREATE TABLE view_table (test_column INT(10)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');

CREATE VIEW test_view AS select test_column from view_table;