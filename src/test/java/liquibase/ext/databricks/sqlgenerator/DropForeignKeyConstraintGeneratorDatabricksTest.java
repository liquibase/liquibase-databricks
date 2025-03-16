package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DropForeignKeyConstraintStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DropForeignKeyConstraintGeneratorDatabricksTest {

    @Mock
    private SqlGeneratorChain<DropForeignKeyConstraintStatement> sqlGeneratorChain;

    @Mock
    private DatabricksDatabase database;

    @InjectMocks
    private DropForeignKeyConstraintGeneratorDatabricks generator;

    private static final String TEST_CATALOG_NAME = "main";
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final String TEST_CONSTRAINT_NAME = "fk_constraint";
    private static final String TEST_CONSTRAINT_NAME_UPPERCASE = "FK_CONSTRAINT";

    private DropForeignKeyConstraintStatement statement;

    @BeforeEach
    void setUp() {
        statement = new DropForeignKeyConstraintStatement(
                TEST_CATALOG_NAME,
                TEST_SCHEMA_NAME,
                TEST_TABLE_NAME,
                TEST_CONSTRAINT_NAME
        );

        // Mock database escaping behavior
        when(database.escapeTableName(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String catalog = invocation.getArgument(0);
                    String schema = invocation.getArgument(1);
                    String table = invocation.getArgument(2);
                    return (catalog != null ? catalog + "." : "") +
                            (schema != null ? schema + "." : "") +
                            table;
                });
        
        when(database.escapeConstraintName(anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void dropForeignKeyWithLowercaseName() {
        // Act
        Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        // Assert
        String sql = result[0].toSql();
        assertEquals("ALTER TABLE main.test_schema.test_table DROP CONSTRAINT fk_constraint", sql);
    }

    @Test
    void dropForeignKeyWithUppercaseName() {
        // Arrange - Create a statement with uppercase constraint name
        DropForeignKeyConstraintStatement upperCaseStatement = new DropForeignKeyConstraintStatement(
                TEST_CATALOG_NAME,
                TEST_SCHEMA_NAME,
                TEST_TABLE_NAME,
                TEST_CONSTRAINT_NAME_UPPERCASE
        );
        
        // Act
        Sql[] result = generator.generateSql(upperCaseStatement, database, sqlGeneratorChain);
        
        // Assert
        String sql = result[0].toSql();
        
        // Verify the constraint name is converted to lowercase
        assertTrue(sql.contains("DROP CONSTRAINT fk_constraint"));
        assertEquals("ALTER TABLE main.test_schema.test_table DROP CONSTRAINT fk_constraint", sql);
    }
}