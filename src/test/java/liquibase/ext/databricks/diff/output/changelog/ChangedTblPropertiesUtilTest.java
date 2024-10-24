package liquibase.ext.databricks.diff.output.changelog;

import liquibase.diff.Difference;
import liquibase.diff.output.DiffOutputControl;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ChangedTblPropertiesUtilTest {

    private final Table table = new Table("catalogName", "schemaName", "tableName");
    private final DiffOutputControl control = new DiffOutputControl();

    @Test
    void justAdd() {
        //Arrange
        Difference difference = new Difference("tblProperties","'this.should.be.added'=35", "");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("'this.should.be.added'=35", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("", result.getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void justRemove() {
        //Arrange
        Difference difference = new Difference("tblProperties", "", "'this.should.be.removed'=true");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("'this.should.be.removed'", result.getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void addAndRemoveAtSameTime() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.added'=35", "'this.should.be.removed'=true");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("'this.should.be.added'=35", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("'this.should.be.removed'", result.getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void modify() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.changed'=35", "'this.should.be.changed'=20");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("'this.should.be.changed'=35", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("", result.getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void ignore() {
        //This case should not ever get to the Change Generator as there is no difference here
        Difference difference = new Difference("tblProperties",
                "'this.should.be.ignored'=35", "'this.should.be.ignored'=35");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("", result.getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void addAndRemoveAndChangeManyAtSameTimeAndInRandomOrder() {
        //Arrange
        Difference difference = new Difference("tblProperties",
                "'this.should.be.ignored'=true,'this.should.be.added.too'=true,'this.should.be.added'=35,'this.should.be.changed'=true", "'this.should.be.changed'=false,'this.should.be.removed'='aaa','this.should.be.ignored'=true,this.should.be.removed.too'=bye");

        //Act
        AbstractAlterPropertiesChangeDatabricks result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals("'this.should.be.added.too'=true,'this.should.be.changed'=true,'this.should.be.added'=35", result.getSetExtendedTableProperties().getTblProperties());
        assertEquals("'this.should.be.removed',this.should.be.removed.too'", result.getUnsetExtendedTableProperties().getTblProperties());
    }
}
