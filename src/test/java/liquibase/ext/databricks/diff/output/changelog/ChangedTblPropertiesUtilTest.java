package liquibase.ext.databricks.diff.output.changelog;

import liquibase.diff.Difference;
import liquibase.diff.output.DiffOutputControl;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesChangeDatabricks;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
    void handleTblPropertiesWithComments() {
        //Arrange
        String tblProperties = "'comment'='The bu_details table contains information about various business units within the organization. " +
                "It includes details such as the name of the business unit. This data can be used to identify and categorize different business units, as well as to track their performance and progress over time. " +
                "It can also be used to facilitate communication and collaboration between different business units.'," +
                "'key1'='value1','key2'='a=b','key3'='another=value','key4'=\"a=b\", 'key5'='c'";

        //Act
        Map<String, String> parcedMap = ChangedTblPropertiesUtil.convertToMapExcludingDeltaParameters(tblProperties);

        //Assert
        assertEquals(6, parcedMap.size());
        assertEquals("'value1'", parcedMap.get("'key1'"));
        assertEquals("'a=b'", parcedMap.get("'key2'"));
        assertEquals("'another=value'", parcedMap.get("'key3'"));
        assertEquals("\"a=b\"", parcedMap.get("'key4'"));
        assertEquals("'c'", parcedMap.get("'key5'"));
        assertEquals("'The bu_details table contains information about various business units within the organization. It includes details such as the name of the business unit. This data can be used to identify and categorize different business units, as well as to track their performance and progress over time. It can also be used to facilitate communication and collaboration between different business units.'", parcedMap.get("'comment'"));
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

    @Test
    void allowSpecificDeltaProperties() {
        // Tests a mix of allowed and disallowed delta properties
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'=true, 'delta.feature.allowColumnDefaults'=true, 'delta.someOtherProperty'=false",
                "'delta.columnMapping.mode'='id', 'delta.enableDeletionVectors'=false, 'delta.invalidProperty'=true");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);

        Set<String> expectedProperties = new TreeSet<>(
                Arrays.asList("'delta.columnMapping.mode'='name'", "'delta.enableDeletionVectors'=true", "'delta.feature.allowColumnDefaults'=true"));

        Set<String> actualProperties = Arrays.stream(
                        result[0].getSetExtendedTableProperties().getTblProperties().split(","))
                .map(String::trim)
                .collect(Collectors.toSet());

        // Checks if only allowed properties are included
        assertEquals(expectedProperties, actualProperties);
    }

    @Test
    void allowAllWhitelistedDeltaProperties() {
        // We test all properties on the whitelist
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.mode'='name', " +
                        "'delta.enableDeletionVectors'=true, " +
                        "'delta.feature.allowColumnDefaults'=true,", "");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        // Checks whether all whitelist properties have been maintained
        assertTrue(result[0].getSetExtendedTableProperties().getTblProperties().contains("'delta.columnMapping.mode'='name'"));
        assertTrue(result[0].getSetExtendedTableProperties().getTblProperties().contains("'delta.enableDeletionVectors'=true"));
        assertTrue(result[0].getSetExtendedTableProperties().getTblProperties().contains("'delta.feature.allowColumnDefaults'=true"));
    }

    @Test
    void mixDeltaAndRegularProperties() {
        // Tests a mix of allowed delta properties and regular properties
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.mode'='name', 'regular.property'=true, 'delta.enableDeletionVectors'=true",
                "'delta.columnMapping.mode'='id', 'other.property'=false");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.length);

        // Verifies that both permitted and regular delta properties were processed correctly
        assertTrue(result[0].getSetExtendedTableProperties().getTblProperties()
                .contains("'delta.columnMapping.mode'='name'"));
        assertTrue(result[0].getSetExtendedTableProperties().getTblProperties()
                .contains("'regular.property'=true"));
        assertEquals("'other.property'", result[1].getUnsetExtendedTableProperties().getTblProperties());
    }

    @Test
    void updateWhitelistedDeltaProperties() {
        // Tests updating values for allowed delta properties
        Difference difference = new Difference("tblProperties",
                "'delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'=true",
                "'delta.columnMapping.mode'='id', 'delta.enableDeletionVectors'=false");

        // Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.length);
        // Checks whether the new values were applied correctly
        String expectedNormalized = "'delta.columnMapping.mode'='name','delta.enableDeletionVectors'=true";
        String actualNormalized = result[0].getSetExtendedTableProperties().getTblProperties();
        assertEquals(expectedNormalized, actualNormalized);
    }
}
