package liquibase.ext.databricks.diff.output.changelog;

import liquibase.diff.Difference;
import liquibase.diff.output.DiffOutputControl;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesChangeDatabricks;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class ChangedTblPropertiesUtilTest {

    private final Table table = new Table("catalogName", "schemaName", "tableName");
    private final DiffOutputControl control = new DiffOutputControl();

    @Test
    void justAdd() {
        //Arrange
        Difference difference = new Difference("tblProperties", "'this.should.be.added'=35", "");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(table.getName(), ((AlterTablePropertiesChangeDatabricks) result[0]).getTableName());
        assertEquals("'this.should.be.added'=35", result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
    }

    @Test
    void justRemove() {
        //Arrange
        Difference difference = new Difference("tblProperties", "", "'this.should.be.removed'=true");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertNull(result[0].getSetExtendedTableProperties());
        assertEquals("'this.should.be.removed'", result[0].getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void addAndRemoveAtSameTime() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.added'=35", "'this.should.be.removed'=true");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals("'this.should.be.added'=35", result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
        assertEquals("'this.should.be.removed'", result[1].getUnsetExtendedTableProperties().getTblProperties());
        assertNull(result[1].getSetExtendedTableProperties());
    }

    @Test
    void modify() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.changed'=35", "'this.should.be.changed'=20");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("'this.should.be.changed'=35", result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
    }

    @Test
    void ignore() {
        //This case should not ever get to the Change Generator as there is no difference here
        Difference difference = new Difference("tblProperties",
                "'this.should.be.ignored'=35", "'this.should.be.ignored'=35");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void addAndRemoveAndChangeManyAtSameTimeAndInRandomOrder() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.ignored'=true,'this.should.be.added.too'=true,'this.should.be.added'=35,'this.should.be.changed'=true", 
                "'this.should.be.changed'=false,'this.should.be.removed'='aaa','this.should.be.ignored'=true,'this.should.be.removed.too'=bye");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals("'this.should.be.added.too'=true,'this.should.be.changed'=true,'this.should.be.added'=35",
                result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
        assertEquals("'this.should.be.removed','this.should.be.removed.too'", result[1].getUnsetExtendedTableProperties().getTblProperties());
        assertNull(result[1].getSetExtendedTableProperties());
    }

    @Test
    void deltaPropertiesHandledLikeRegularProperties() {
        // Tests that delta properties are treated like any other property (no special filtering)
        // Combines: add, remove, modify, and mix of delta and regular properties
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.mode'='name', 'regular.property'=true, 'delta.enableDeletionVectors'=true, 'delta.customProperty'=false",
                "'delta.columnMapping.mode'='id', 'other.property'=false, 'delta.oldProperty'=true");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.length);

        // Verify SET operations (add new + modify existing)
        String setProperties = result[0].getSetExtendedTableProperties().getTblProperties();
        assertTrue(setProperties.contains("'delta.columnMapping.mode'='name'"));
        assertTrue(setProperties.contains("'regular.property'=true"));
        assertTrue(setProperties.contains("'delta.enableDeletionVectors'=true"));
        assertTrue(setProperties.contains("'delta.customProperty'=false"));
        assertNull(result[0].getUnsetExtendedTableProperties());

        // Verify UNSET operations (remove properties not in reference)
        String unsetProperties = result[1].getUnsetExtendedTableProperties().getTblProperties();
        assertTrue(unsetProperties.contains("'other.property'"));
        assertTrue(unsetProperties.contains("'delta.oldProperty'"));
        assertNull(result[1].getSetExtendedTableProperties());
    }

    @Test
    void complexArrayPropertyParsing() {
        // Test parsing of complex array properties like clusteringColumns
        Difference difference = new Difference("tblProperties",
                "clusteringColumns=[[\"column_1\"],[\"column_2\"],[\"column_3\"]]",
                "");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("clusteringColumns=[[\"column_1\"],[\"column_2\"],[\"column_3\"]]", 
                result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
    }


    @Test
    void complexCommaSeparatedPropertyParsing() {
        // Test parsing of comma-separated properties like delta.dataSkippingStatsColumns
        Difference difference = new Difference("tblProperties",
                "delta.dataSkippingStatsColumns=column_1,column_2,column_3,column_4",
                "");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("delta.dataSkippingStatsColumns=column_1,column_2,column_3,column_4", 
                result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
    }

    @Test
    void mixedComplexAndSimpleProperties() {
        // Test parsing when complex and simple properties are mixed
        Difference difference = new Difference("tblProperties",
                "simple.property=true,clusteringColumns=[[\"column_1\"],[\"column_2\"]],delta.dataSkippingStatsColumns=COL_1,COL_2,COL_3,another.simple=false",
                "");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        String setProperties = result[0].getSetExtendedTableProperties().getTblProperties();
        assertTrue(setProperties.contains("simple.property=true"));
        assertTrue(setProperties.contains("clusteringColumns=[[\"column_1\"],[\"column_2\"]]"));
        assertTrue(setProperties.contains("delta.dataSkippingStatsColumns=COL_1,COL_2,COL_3"));
        assertTrue(setProperties.contains("another.simple=false"));
        assertNull(result[0].getUnsetExtendedTableProperties());
    }
}
