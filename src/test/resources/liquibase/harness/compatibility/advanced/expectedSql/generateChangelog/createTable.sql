CREATE TABLE authors_data (authors_email STRING(255) NOT NULL, CONSTRAINT pk_authors_email PRIMARY KEY (authors_email)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
CREATE TABLE test_table (test_column INT(10), varcharColumn STRING(255), intColumn INT(10), dateColumn date) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
CREATE TABLE test_table_base (id INT(10) NOT NULL, CONSTRAINT pk_test_table_base PRIMARY KEY (id)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
CREATE TABLE test_table_reference (id INT(10), test_column INT(10) NOT NULL, CONSTRAINT pk_test_table_reference PRIMARY KEY (test_column)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
CREATE TABLE test_table_xml (test_column INT(10)) USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')