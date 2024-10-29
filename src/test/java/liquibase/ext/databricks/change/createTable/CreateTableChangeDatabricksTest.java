package liquibase.ext.databricks.change.createTable;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.core.CreateTableStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateTableChangeDatabricksTest {

    private static final String EXPECTED_TABLE_FORMAT = "delta";
    private static final String EXPECTED_TABLE_LOCATION = "Location";
    private static final String EXPECTED_PARTITION_COLUMNS = "col1,col2";
    private static final String EXPECTED_CLUSTER_COLUMNS = "col2,col3";
    private static final String EXPECTED_TABLE_NAME = "test_table";
    private static final String CLUSTER_AND_PARTITION_ERROR_MESSAGE = "Databricks does not support CLUSTER columns AND " +
            "PARTITION BY columns, please pick one.";
    private static final String DOUBLE_INITIALIZATION_ERROR_MESSAGE = "Double initialization of extended table properties is not allowed. " +
            "Please avoid using both EXT createTable attributes and Databricks specific extendedTableProperties element. " +
            "Element databricks:extendedTableProperties is preferred way to set databricks specific configurations.";

    private CreateTableChangeDatabricks createTableChangeDatabricks;
    private Database database = new DatabricksDatabase();

    @BeforeEach
    void setup() {
        createTableChangeDatabricks = new CreateTableChangeDatabricks();
        createTableChangeDatabricks.setTableFormat(EXPECTED_TABLE_FORMAT);
        createTableChangeDatabricks.setTableLocation(EXPECTED_TABLE_LOCATION);
        createTableChangeDatabricks.setPartitionColumns(EXPECTED_PARTITION_COLUMNS);
        createTableChangeDatabricks.setTableName(EXPECTED_TABLE_NAME);
        ColumnConfig columnConfig = new ColumnConfig();
        columnConfig.setType("INT");
        columnConfig.setName("id");
        columnConfig.setComputed(false);
        createTableChangeDatabricks.setColumns(Collections.singletonList(columnConfig));
    }

    @Test
    void testValidate_shouldFailWithAmbiguousClusterOrPartition() {
        createTableChangeDatabricks.setClusterColumns(EXPECTED_CLUSTER_COLUMNS);

        ValidationErrors validationErrors = createTableChangeDatabricks.validate(database);
        assertNotNull(validationErrors);
        assertTrue(validationErrors.hasErrors());
        assertFalse(validationErrors.getErrorMessages().isEmpty());
        assertEquals(CLUSTER_AND_PARTITION_ERROR_MESSAGE, validationErrors.getErrorMessages().get(0));
    }

    @Test
    void testValidate_shouldFailWithDoublePropertyInitialization() {
        ExtendedTableProperties extendedTableProperties = new ExtendedTableProperties();
        extendedTableProperties.setPartitionColumns(EXPECTED_PARTITION_COLUMNS);
        createTableChangeDatabricks.setExtendedTableProperties(extendedTableProperties);

        ValidationErrors validationErrors = createTableChangeDatabricks.validate(database);
        assertNotNull(validationErrors);
        assertTrue(validationErrors.hasErrors());
        assertFalse(validationErrors.getErrorMessages().isEmpty());
        assertEquals(DOUBLE_INITIALIZATION_ERROR_MESSAGE, validationErrors.getErrorMessages().get(0));
    }

    @Test
    void testGenerateCreateTableStatement_noExtendedTableProperties() {
        CreateTableStatement createTableStatement = createTableChangeDatabricks.generateCreateTableStatement();
        assertTrue(createTableStatement instanceof CreateTableStatementDatabricks);
        CreateTableStatementDatabricks databricksStatement = (CreateTableStatementDatabricks) createTableStatement;
        assertNull(databricksStatement.getExtendedTableProperties());
        assertEquals(EXPECTED_TABLE_FORMAT, databricksStatement.getTableFormat());
        assertEquals(EXPECTED_TABLE_LOCATION, databricksStatement.getTableLocation());
        assertEquals(new ArrayList<>(), databricksStatement.getClusterColumns());
        String actualPartitionColumns = databricksStatement.getPartitionColumns()
                .stream().reduce("", (col1, col2) -> col1 + ',' + col2).substring(1);
        assertEquals(EXPECTED_PARTITION_COLUMNS, actualPartitionColumns);
    }

    @Test
    void testGenerateCreateTableStatement_withExtendedTableProperties() {
        createTableChangeDatabricks.setTableFormat(null);
        createTableChangeDatabricks.setTableLocation(null);
        createTableChangeDatabricks.setPartitionColumns(null);
        ExtendedTableProperties extendedTableProperties = new ExtendedTableProperties();
        extendedTableProperties.setTableFormat(EXPECTED_TABLE_FORMAT);
        extendedTableProperties.setTableLocation(EXPECTED_TABLE_LOCATION);
        extendedTableProperties.setClusterColumns(EXPECTED_CLUSTER_COLUMNS);
        createTableChangeDatabricks.setExtendedTableProperties(extendedTableProperties);

        CreateTableStatement createTableStatement = createTableChangeDatabricks.generateCreateTableStatement();
        assertTrue(createTableStatement instanceof CreateTableStatementDatabricks);
        CreateTableStatementDatabricks databricksStatement = (CreateTableStatementDatabricks) createTableStatement;
        assertNotNull(databricksStatement.getExtendedTableProperties());
        assertEquals(EXPECTED_TABLE_FORMAT, databricksStatement.getTableFormat());
        assertEquals(EXPECTED_TABLE_LOCATION, databricksStatement.getTableLocation());
        String actualClusterColumns = databricksStatement.getClusterColumns()
                .stream().reduce("", (col1, col2) -> col1 + ',' + col2).substring(1);
        assertEquals(EXPECTED_CLUSTER_COLUMNS, actualClusterColumns);
        assertEquals(new ArrayList<>(), databricksStatement.getPartitionColumns());
    }
}