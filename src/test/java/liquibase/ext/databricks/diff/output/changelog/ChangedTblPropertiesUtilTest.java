package liquibase.ext.databricks.diff.output.changelog;

import liquibase.diff.Difference;
import liquibase.diff.output.DiffOutputControl;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterViewProperties.AlterViewPropertiesChangeDatabricks;
import liquibase.structure.core.Table;
import liquibase.structure.core.View;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
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
                "'this.should.be.ignored'=true,'this.should.be.added.too'=true,'this.should.be.added'=35,'this.should.be.changed'=true", "'this.should.be" +
                ".changed'=false,'this.should.be.removed'='aaa','this.should.be.ignored'=true,this.should.be.removed.too'=bye");

        //Act
        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        //Assert
        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals("'this.should.be.added.too'=true,'this.should.be.changed'=true,'this.should.be.added'=35",
                result[0].getSetExtendedTableProperties().getTblProperties());
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

    // === New tests to improve mutation coverage ===

    @Test
    void nullReferenceValueShouldProduceEmptyMap() {
        // Tests the null branch of convertToMapExcludingDeltaParameters (line 107)
        Difference difference = new Difference("tblProperties", null, "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void nullComparedValueShouldProduceEmptyMap() {
        // Tests the null branch for compared value
        Difference difference = new Difference("tblProperties", "'key'=value", null);

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("'key'=value", result[0].getSetExtendedTableProperties().getTblProperties());
    }

    @Test
    void whitespaceAroundKeysAndValuesShouldBeTrimmed() {
        // Tests the trim() calls on line 111 (NakedReceiverMutator + InlineConstantMutator)
        Difference difference = new Difference("tblProperties", " 'key' = value ", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("'key'=value", result[0].getSetExtendedTableProperties().getTblProperties());
    }

    @Test
    void withIncludeCatalogAndSchemaShouldSetNames() {
        // Tests the includeCatalog and includeSchema branches (lines 134, 138)
        DiffOutputControl controlWithCatalogAndSchema = new DiffOutputControl(true, true, true, null);
        Difference difference = new Difference("tblProperties", "'key'=value", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, controlWithCatalogAndSchema, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("catalogName", result[0].getCatalogName());
        assertEquals("schemaName", result[0].getSchemaName());
    }

    @Test
    void withIncludeCatalogOnlyShouldSetCatalogName() {
        // Tests includeCatalog=true, includeSchema=false (line 134 true, line 138 false)
        DiffOutputControl controlCatalogOnly = new DiffOutputControl(true, false, true, null);
        Difference difference = new Difference("tblProperties", "'key'=value", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, controlCatalogOnly, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("catalogName", result[0].getCatalogName());
        assertNull(result[0].getSchemaName());
    }

    @Test
    void withIncludeSchemaOnlyShouldSetSchemaName() {
        // Tests includeCatalog=false, includeSchema=true (line 134 false, line 138 true)
        DiffOutputControl controlSchemaOnly = new DiffOutputControl(false, true, true, null);
        Difference difference = new Difference("tblProperties", "'key'=value", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, controlSchemaOnly, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertNull(result[0].getCatalogName());
        assertEquals("schemaName", result[0].getSchemaName());
    }

    @Test
    void withNeitherCatalogNorSchemaShouldNotSetNames() {
        // Tests includeCatalog=false, includeSchema=false (both lines 134, 138 false)
        DiffOutputControl controlNeither = new DiffOutputControl(false, false, true, null);
        Difference difference = new Difference("tblProperties", "'key'=value", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, controlNeither, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertNull(result[0].getCatalogName());
        assertNull(result[0].getSchemaName());
    }

    @Test
    void getAlterViewPropertiesChangeDatabricksShouldWork() {
        // Tests getAlterViewPropertiesChangeDatabricks method (lines 47-49)
        View view = new View("catalogName", "schemaName", "viewName");
        DiffOutputControl control = new DiffOutputControl(true, true, true, null);
        Difference difference = new Difference("tblProperties", "'key'=value", "");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterViewPropertiesChangeDatabricks(view, control, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("viewName", ((AlterViewPropertiesChangeDatabricks) result[0]).getViewName());
        assertEquals("catalogName", result[0].getCatalogName());
        assertEquals("schemaName", result[0].getSchemaName());
    }

    @Test
    void getAlterViewPropertiesChangeDatabricksWithRemoveOnly() {
        // Tests getAlterViewPropertiesChangeDatabricks with remove-only scenario
        View view = new View("catalogName", "schemaName", "viewName");
        DiffOutputControl control = new DiffOutputControl(true, true, true, null);
        Difference difference = new Difference("tblProperties", "", "'key'=value");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterViewPropertiesChangeDatabricks(view, control, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("viewName", ((AlterViewPropertiesChangeDatabricks) result[0]).getViewName());
        assertNull(result[0].getSetExtendedTableProperties());
        assertNotNull(result[0].getUnsetExtendedTableProperties());
    }

    @Test
    void getFilteredTblPropertiesShouldReturnFilteredString() {
        // Tests getFilteredTblProperties method (lines 123-124)
        String result = ChangedTblPropertiesUtil.getFilteredTblProperties("'key1'=value1,'key2'=value2");

        // order-independent: getFilteredTblProperties joins HashMap entries, so entry order is not contractual
        assertEquals(
                new TreeSet<>(Arrays.asList("'key1'=value1", "'key2'=value2")),
                new TreeSet<>(Arrays.asList(result.split(","))));
    }

    @Test
    void getFilteredTblPropertiesShouldExcludeDeltaProperties() {
        // Tests getFilteredTblProperties filters out non-allowed delta properties
        String result = ChangedTblPropertiesUtil.getFilteredTblProperties(
                "'key1'=value1,delta.internalProp=bad,'delta.columnMapping.mode'=name");

        // order-independent: delta.internalProp is filtered out; remaining entry order is not contractual
        assertEquals(
                new TreeSet<>(Arrays.asList("'key1'=value1", "'delta.columnMapping.mode'=name")),
                new TreeSet<>(Arrays.asList(result.split(","))));
    }

    @Test
    void getFilteredTblPropertiesWithEmptyInput() {
        // Tests getFilteredTblProperties with empty string
        String result = ChangedTblPropertiesUtil.getFilteredTblProperties("");

        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    void justRemoveShouldNotSetCatalogOrSchemaWithDefaultControl() {
        // Tests that default DiffOutputControl (includeCatalog=true, includeSchema=true)
        // sets catalog and schema names even for remove-only changes
        Difference difference = new Difference("tblProperties", "", "'key'=value");

        AbstractAlterPropertiesChangeDatabricks[] result = ChangedTblPropertiesUtil
                .getAlterTablePropertiesChangeDatabricks(table, control, difference);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertNull(result[0].getSetExtendedTableProperties());
        assertNotNull(result[0].getUnsetExtendedTableProperties());
        assertEquals("catalogName", result[0].getCatalogName());
        assertEquals("schemaName", result[0].getSchemaName());
    }
}
