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
        Difference difference = new Difference("tblProperties","'this.should.be.added'=35", "");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(table.getName(), ((AlterTablePropertiesChangeDatabricks)result[0]).getTableName());
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
                "'this.should.be.ignored'=true,'this.should.be.added.too'=true,'this.should.be.added'=35,'this.should.be.changed'=true", "'this.should.be.changed'=false,'this.should.be.removed'='aaa','this.should.be.ignored'=true,this.should.be.removed.too'=bye");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals("'this.should.be.added.too'=true,'this.should.be.changed'=true,'this.should.be.added'=35", result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
        assertEquals("'this.should.be.removed',this.should.be.removed.too'", result[1].getUnsetExtendedTableProperties().getTblProperties());
        assertNull(result[1].getSetExtendedTableProperties());
    }

    @Test
    void ignoreInternalDeltaProperties() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.maxColumnId'=35, 'this.should.be.added.too'=true",
                "'this.should.be.dropped'=false, 'delta.feature.clustering'=20");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals("'this.should.be.added.too'=true", result[0].getSetExtendedTableProperties().getTblProperties());
        assertNull(result[0].getUnsetExtendedTableProperties());
        assertEquals("'this.should.be.dropped'", result[1].getUnsetExtendedTableProperties().getTblProperties());
        assertNull(result[1].getSetExtendedTableProperties());
    }
}
