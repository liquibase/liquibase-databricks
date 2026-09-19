package liquibase.ext.databricks.sqlgenerator;

import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BigIntType;
import liquibase.datatype.core.TimestampType;
import liquibase.datatype.core.VarcharType;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.createTable.CreateTableStatementDatabricks;
import liquibase.ext.databricks.change.createTable.ExtendedTableProperties;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateTableStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateTableGeneratorDatabricksTest {

    @Mock
    private SqlGeneratorChain<CreateTableStatement> sqlGeneratorChain;

    @Mock
    private DatabricksDatabase database;

    @InjectMocks
    private CreateTableGeneratorDatabricks generator;

    private static final String TEST_CATALOG_NAME = "main";
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test_table";

    private static final String DEFAULT_PROPERTIES =
            "'delta.columnMapping.mode' = 'name', " +
                    "'delta.enableDeletionVectors' = true, " +
                    "'delta.feature.allowColumnDefaults' = 'supported'";

    private CreateTableStatementDatabricks statement;

    @BeforeEach
    void setUp() {
        statement = new CreateTableStatementDatabricks(
                TEST_CATALOG_NAME,
                TEST_SCHEMA_NAME,
                TEST_TABLE_NAME
        );

        // Initializing lists to avoid NullPointerException
        statement.setClusterColumns(null);
        statement.setPartitionColumns(null);

        // Initial configuration of extended properties
        ExtendedTableProperties properties = new ExtendedTableProperties();
        statement.setExtendedTableProperties(properties);
    }

    @Test
    void createTableWithDefaultProperties() {

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();

        // check the presence of all standard properties
        for (String property : DEFAULT_PROPERTIES.split(",")) {
            property = property.trim();
            assertTrue(
                    sql.contains(property),
                    "SQL should contain default property: " + property
            );
        }
    }

    @Test
    void createTableWithCustomProperties() {
        // Arrange
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties(
                "'custom.property' = 'value', " +
                        "'delta.columnMapping.mode' = 'custom'"
        );
        statement.setExtendedTableProperties(properties);

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();

        // Check custom properties
        assertTrue(sql.contains("'custom.property' = 'value'"));
        assertTrue(sql.contains("'delta.columnMapping.mode' = 'custom'"));

        // Checks if missing essential properties have been added
        assertTrue(sql.contains("'delta.enableDeletionVectors' = true"));
    }

    @Test
    void createTableWithAllCustomProperties() {
        // Arrange
        ExtendedTableProperties properties = new ExtendedTableProperties();
        //'delta.feature.allowColumnDefaults' support only 'enabled' and 'supported', as for Jan 2025 there is no way to override it to set 'false'
        String customProperties =
                        "'delta.columnMapping.mode' = 'custom', " +
                        "'delta.enableDeletionVectors' = false";

        properties.setTblProperties(customProperties);
        statement.setExtendedTableProperties(properties);

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();

        // Checks if all custom values were maintained
        for (String property : customProperties.split(",")) {
            property = property.trim();
            assertTrue(
                    sql.contains(property),
                    "SQL should contain custom property: " + property
            );
        }
    }

    @Test
    void createTableWithLocationAndFormat() {
        // Arrange
        statement.setTableLocation("/path/to/table");
        statement.setTableFormat("delta");
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties("'custom.property' = 'value'");
        statement.setExtendedTableProperties(properties);

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();
        assertTrue(sql.contains("LOCATION '/path/to/table'"));
        assertTrue(sql.contains("USING delta"));
        assertTrue(sql.contains("'custom.property' = 'value'"));

        // Checks whether essential properties are still present
        assertTrue(sql.contains("'delta.columnMapping.mode'"));
    }

    @Test
    void createTableWithClusterColumns() {
        // Arrange
        statement.setClusterColumns("col1, col2");

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();
        assertTrue(sql.contains("CLUSTER BY (col1, col2)"));
    }

    @Test
    void createTableWithPartitionColumns() {
        // Arrange

        LiquibaseDataType bigintType = new BigIntType();
        statement.addColumn("id", bigintType);

        statement.setPartitionColumns("part1,part2");

        // Check that there are no cluster columns (they are mutually exclusive)
        statement.setClusterColumns(null);

        ExtendedTableProperties properties = new ExtendedTableProperties();
        statement.setExtendedTableProperties(properties);

        // Mock database behavior for column type handling
        when(database.escapeTableName(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String catalog = invocation.getArgument(0);
                    String schema = invocation.getArgument(1);
                    String table = invocation.getArgument(2);
                    return (catalog != null ? catalog + "." : "") +
                            (schema != null ? schema + "." : "") +
                            table;
                });
        when(database.escapeColumnName(anyString(), anyString(), anyString(), eq("id"), anyBoolean()))
                .thenReturn("id");

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();

        assertAll(
                () -> assertTrue(sql.contains("CREATE TABLE"), "Should contain CREATE TABLE clause"),
                () -> assertTrue(sql.contains("main.test_schema.test_table"), "Should contain fully qualified table name"),
                () -> assertTrue(sql.contains("USING delta"), "Should specify delta format"),
                () -> assertTrue(sql.contains("PARTITIONED BY"), "Should contain PARTITION BY clause"),
                () -> assertTrue(sql.contains("part1"), "Should contain first partition column"),
                () -> assertTrue(sql.contains("part2"), "Should contain second partition column"),
                () -> assertTrue(sql.contains("(id BIGINT)"), "Should contain column definition")
        );

        assertTrue(
                sql.indexOf("USING delta") > sql.indexOf("test_table"),
                "USING delta should appear after table name"
        );
    }

    @Test
    void verifyCompleteCreateTableWithPartitions() {
        // Arrange
        statement.setPartitionColumns("part1, part2");
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties("'custom.property'='value'");
        statement.setExtendedTableProperties(properties);

        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);

        // Assert
        String sql = result[0].toSql();

        assertAll(
                () -> assertTrue(sql.contains("CREATE TABLE"), "Should contain CREATE TABLE"),
                () -> assertTrue(sql.contains("USING delta"), "Should contain USING delta"),
                () -> assertTrue(sql.contains("PARTITIONED BY"), "Should contain PARTITIONED BY"),
                () -> assertTrue(sql.contains("part1"), "Should contain first partition column"),
                () -> assertTrue(sql.contains("part2"), "Should contain second partition column"),
                () -> assertTrue(sql.contains("TBLPROPERTIES"), "Should contain TBLPROPERTIES")
        );
    }

    @Test
    void createTableWithMultipleColumnsAndTypes() {
        // Adding different column types with constraints
        statement.addColumn("id", new BigIntType());
        statement.addColumn("name", new VarcharType());
        statement.addColumn("created_at", new TimestampType());

        // Adding NOT NULL constraint using the correct method
        statement.addColumnConstraint(new NotNullConstraint("id"));

        // Set up the necessary mocking for column name escaping
        when(database.escapeColumnName(anyString(), anyString(), anyString(), eq("id"), anyBoolean()))
                .thenReturn("id");
        when(database.escapeColumnName(anyString(), anyString(), anyString(), eq("name"), anyBoolean()))
                .thenReturn("name");
        when(database.escapeColumnName(anyString(), anyString(), anyString(), eq("created_at"), anyBoolean()))
                .thenReturn("created_at");

        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        String sql = result[0].toSql();

        assertAll(
                () -> assertTrue(sql.contains("id BIGINT"), "Should contain id column definition"),
                () -> assertTrue(sql.contains("name VARCHAR"), "Should contain name column definition"),
                () -> assertTrue(sql.contains("created_at timestamp"), "Should contain timestamp column definition"),
                () -> assertTrue(sql.contains("NOT NULL"), "Should contain NOT NULL constraint")
        );
    }

    @Test
    void validateErrorWhenBothPartitionAndClusterColumnsArePresent() {
        // Testing the validation error when both partition and cluster columns are set
        statement.setPartitionColumns("part1, part2");
        statement.setClusterColumns("col1, col2");

        ValidationErrors errors = generator.validate(statement, database, sqlGeneratorChain);

        assertTrue(errors.hasErrors());
        assertTrue(errors.getErrorMessages().get(0).contains("Databricks does not supported creating tables"));
    }

    @Test
    void createTableWithEmptyProperties() {
        // Testing how the generator handles empty property strings
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties("");
        statement.setExtendedTableProperties(properties);

        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        String sql = result[0].toSql();

        // Should still contain all default properties
        for (String property : DEFAULT_PROPERTIES.split(",")) {
            property = property.trim();
            assertTrue(
                    sql.contains(property),
                    "SQL should contain default property even with empty properties string: " + property
            );
        }
    }

    @Test
    void createTableWithDifferentPropertyValueTypes() {
        // Testing different types of property values (strings, numbers, booleans)
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties(
                "'string.prop' = 'value', " +
                        "'number.prop' = 123, " +
                        "'boolean.prop' = false"
        );
        statement.setExtendedTableProperties(properties);

        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        String sql = result[0].toSql();

        assertAll(
                () -> assertTrue(sql.contains("'string.prop' = 'value'"), "Should handle string properties"),
                () -> assertTrue(sql.contains("'number.prop' = 123"), "Should handle numeric properties"),
                () -> assertTrue(sql.contains("'boolean.prop' = false"), "Should handle boolean properties")
        );
    }

    @Test
    void createTableWithComplexProperties() {
        // Test the fix for complex properties that contain commas
        ExtendedTableProperties properties = new ExtendedTableProperties();
        properties.setTblProperties(
                "simple.property=true,clusteringColumns=[[\"column_1\"],[\"column_2\"]],delta.dataSkippingStatsColumns=COL_1,COL_2,COL_3,another.simple=false"
        );
        statement.setExtendedTableProperties(properties);

        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        String sql = result[0].toSql();

        // Verify that all properties are correctly parsed and included
        assertAll(
                () -> assertTrue(sql.contains("simple.property = true"), "Should handle simple property"),
                () -> assertTrue(sql.contains("clusteringColumns = [[\"column_1\"],[\"column_2\"]]"), "Should handle complex array property"),
                () -> assertTrue(sql.contains("delta.dataSkippingStatsColumns = COL_1,COL_2,COL_3"), "Should handle comma-separated property values"),
                () -> assertTrue(sql.contains("another.simple = false"), "Should handle property after complex value")
        );
    }
}