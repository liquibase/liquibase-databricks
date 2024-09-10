package liquibase.ext.databricks.change.addLookupTable;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddForeignKeyConstraintStatement;
import liquibase.statement.core.AddPrimaryKeyStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.SetNullableStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AddLookupTableChangeDatabricksTest {

    private DatabricksDatabase database;

    @BeforeEach
    public void setUp() {
        database = new DatabricksDatabase();
    }

    @Test
    void getConstraintName() {
        AddLookupTableChangeDatabricks change = new AddLookupTableChangeDatabricks();
        change.setConstraintName("testConstraint");

        assertEquals("testConstraint", change.getFinalConstraintName());

    }

    @Test
    void getConstraintNameGenerated() {
        AddLookupTableChangeDatabricks changeNoName = new AddLookupTableChangeDatabricks();
        changeNoName.setExistingTableName("existingTable");
        changeNoName.setNewTableName("newTable");

        assertEquals("fk_existingtable_newtable", changeNoName.getFinalConstraintName());

    }

    @Test
    void supports() {
        AddLookupTableChangeDatabricks change = new AddLookupTableChangeDatabricks();
        assertTrue(change.supports(database));
    }

    @Test
    void generateStatements() {
        AddLookupTableChangeDatabricks change = new AddLookupTableChangeDatabricks();
        change.setExistingTableName("oldTable");
        change.setExistingColumnName("oldColumn");
        change.setNewTableName("newTable");
        change.setNewColumnName("newColumn");
        change.setNewColumnDataType("string");

        SqlStatement[] sqlStatements = change.generateStatements(database);
        assertEquals(4, sqlStatements.length);
        assertInstanceOf(RawParameterizedSqlStatement.class, sqlStatements[0]);
        assertEquals("CREATE TABLE newTable USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')" +
                "  AS SELECT DISTINCT oldColumn AS newColumn FROM oldTable WHERE oldColumn IS NOT NULL",
                ((RawParameterizedSqlStatement) sqlStatements[0]).getSql() );
        assertInstanceOf(SetNullableStatement.class, sqlStatements[1]);
        assertInstanceOf(AddPrimaryKeyStatement.class, sqlStatements[2]);
        assertInstanceOf(AddForeignKeyConstraintStatement.class, sqlStatements[3]);
    }
}