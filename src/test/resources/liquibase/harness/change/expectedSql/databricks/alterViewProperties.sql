CREATE VIEW main.liquibase_harness_test_ds.test_alter_view_properties AS select id, first_name, last_name, email from authors
ALTER VIEW main.liquibase_harness_test_ds.test_alter_view_properties SET TBLPROPERTIES ('external.location'='s3://mybucket/mytable','this.is.my.key'=12,'this.is.my.key2'=true)
