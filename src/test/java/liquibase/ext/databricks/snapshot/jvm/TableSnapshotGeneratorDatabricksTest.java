package liquibase.ext.databricks.snapshot.jvm;

import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableSnapshotGeneratorDatabricksTest {

    @Test
    void getQualifiedTableNameTableWithDashesTest() {

        Database database = new DatabricksDatabase();
        String catalogName = "main";
        String shemaName = "liquibase_harness_test_ds";
        String simpleTableName = "myTable";
        String tableNameWithDashes = "random_table-of-stuff";
        Table simpleTable = new Table(catalogName, shemaName, simpleTableName);
        Table tableWithDashes = new Table(catalogName, shemaName, tableNameWithDashes);
        String qualifiedSimpleTableName = TableSnapshotGeneratorDatabricks.getQualifiedTableName(simpleTable, database);
        String qualifiedTableNameWithDashes = TableSnapshotGeneratorDatabricks.getQualifiedTableName(tableWithDashes, database);
        assertEquals("main.liquibase_harness_test_ds.myTable", qualifiedSimpleTableName);
        assertEquals("main.liquibase_harness_test_ds.`random_table-of-stuff`", qualifiedTableNameWithDashes);
    }
}
