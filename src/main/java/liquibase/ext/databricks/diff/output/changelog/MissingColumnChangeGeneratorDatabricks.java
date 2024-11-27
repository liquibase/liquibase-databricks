package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.AddColumnConfig;
import liquibase.change.Change;
import liquibase.change.ConstraintsConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.AddDefaultValueChange;
import liquibase.change.core.AddNotNullConstraintChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.MissingColumnChangeGenerator;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Custom diff change generator for Databricks
 */
public class MissingColumnChangeGeneratorDatabricks extends MissingColumnChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && super.getPriority(objectType, database) > PRIORITY_NONE) {
            return PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        Change[] changes = super.fixMissing(missingObject, control, referenceDatabase, comparisonDatabase, chain);
        return changes == null ? null : handleMissingColumnConstraints((Column) missingObject, control, changes);
    }

    private Change[] handleMissingColumnConstraints(Column column, DiffOutputControl control, Change[] changes) {
        Optional<AddColumnChange> addColumnOptional = Arrays.stream(changes)
                .filter(change -> isCurrentColumnChange(change, column, control))
                .map(AddColumnChange.class::cast).findFirst();
        if(addColumnOptional.isPresent()) {
            AddColumnChange addColumnChange = addColumnOptional.get();
            changes = splitAddColumnChange(column, control, changes, addColumnChange);
        }
        return changes;
    }

    private Change[] splitAddColumnChange(Column column, DiffOutputControl control, Change[] changes, AddColumnChange addColumnChange) {
        List<Change> changeList = new ArrayList<>(Arrays.asList(changes));
        AddColumnConfig addColumnConfig = addColumnChange.getColumns().get(0);
        if(addColumnConfig.getDefaultValue() != null || addColumnConfig.getDefaultValueComputed() != null) {
            AddDefaultValueChange addDefaultValueChange = handleDefaultValue(column, control, addColumnChange);
            changeList.add(addDefaultValueChange);
        }
        if(addColumnConfig.getConstraints() != null && Objects.equals(addColumnConfig.getConstraints().isNullable(), Boolean.FALSE)) {
            AddNotNullConstraintChange addNotNullConstraintChange = handleNotNull(column, control, addColumnChange);
            changeList.add(addNotNullConstraintChange);
        }
        if(constraintsAreEmpty(addColumnConfig, addColumnConfig.getConstraints())) {
            addColumnConfig.setConstraints(null);
        }
        changes = changeList.toArray(new Change[0]);
        return changes;
    }

    private AddDefaultValueChange handleDefaultValue(Column column, DiffOutputControl control, AddColumnChange addColumnChange) {
        AddColumnConfig addColumnConfig = addColumnChange.getColumns().get(0);
        String defaultValue = addColumnConfig.getDefaultValue();
        DatabaseFunction defaultValueComputed = addColumnConfig.getDefaultValueComputed();
        String columnDataType = addColumnConfig.getType();
        addColumnConfig.setDefaultValue(null);
        addColumnConfig.setDefaultValueComputed(null);
        addColumnConfig.setComputed(null);
        AddDefaultValueChange addDefaultValueChange = new AddDefaultValueChange();
        if (control.getIncludeCatalog()) {
            addDefaultValueChange.setCatalogName(column.getRelation().getSchema().getCatalog().getName());
        }
        if (control.getIncludeSchema()) {
            addDefaultValueChange.setSchemaName(column.getRelation().getSchema().getName());
        }
        addDefaultValueChange.setTableName(column.getRelation().getName());
        addDefaultValueChange.setColumnName(column.getName());
        addDefaultValueChange.setColumnDataType(columnDataType);

        if (defaultValueComputed != null) {
            addDefaultValueChange.setDefaultValueComputed(defaultValueComputed);
        } else {
            addDefaultValueChange.setDefaultValue(defaultValue);
        }
        addDefaultValueChange.setDefaultValueConstraintName(column.getDefaultValueConstraintName());
        return addDefaultValueChange;
    }

    private AddNotNullConstraintChange handleNotNull(Column column, DiffOutputControl control, AddColumnChange addColumnChange) {
        AddColumnConfig addColumnConfig = addColumnChange.getColumns().get(0);
        ConstraintsConfig constraints = addColumnConfig.getConstraints();
        constraints.setNullable((Boolean) null);
        constraints.setNullable((String) null);
        constraints.setNotNullConstraintName(null);
        AddNotNullConstraintChange addNotNullConstraintChange = createAddNotNullConstraintChange(addColumnConfig, constraints);
        if (control.getIncludeCatalog()) {
            addNotNullConstraintChange.setCatalogName(column.getRelation().getSchema().getCatalog().getName());
        }
        if (control.getIncludeSchema()) {
            addNotNullConstraintChange.setSchemaName(column.getRelation().getSchema().getName());
        }
        addNotNullConstraintChange.setTableName(column.getRelation().getName());
        return addNotNullConstraintChange;
    }

    private AddNotNullConstraintChange createAddNotNullConstraintChange(AddColumnConfig column, ConstraintsConfig constraints) {
        AddNotNullConstraintChange addNotNullConstraintChange = new AddNotNullConstraintChange();
        addNotNullConstraintChange.setColumnName(column.getName());
        addNotNullConstraintChange.setColumnDataType(column.getType());
        addNotNullConstraintChange.setValidate(constraints.getValidateNullable());
        addNotNullConstraintChange.setConstraintName(constraints.getNotNullConstraintName());
        return addNotNullConstraintChange;
    }

    /**
     * We perform reversed checks that were used in the
     * {@link liquibase.change.core.AddColumnChange#generateStatements(Database)}
     * to make sure there won't be empty constraints generated in generated changelog files.
     * */
    boolean constraintsAreEmpty(AddColumnConfig column, ConstraintsConfig constraints) {
        if(constraints != null) {
            return ObjectUtils.allNull(constraints.isNullable(), constraints.isUnique(), constraints.isPrimaryKey(),
                    column.isAutoIncrement(), constraints.getReferences(), constraints.getReferencedColumnNames(),
                    constraints.getReferencedTableName());
        }
        return column.isAutoIncrement() != null && !column.isAutoIncrement();
    }

    private boolean isCurrentColumnChange(Change change, Column currentColumn, DiffOutputControl control) {
        if(change instanceof AddColumnChange) {
            AddColumnChange addColumnChange = ((AddColumnChange) change);
            AddColumnConfig addColumnConfig = addColumnChange.getColumns().get(0);
            boolean columnNameEqual = addColumnConfig.getName().equals(currentColumn.getName());
            boolean tableNameEqual = addColumnChange.getTableName().equals(currentColumn.getRelation().getName());
            boolean schemaNameEqual = !control.getIncludeSchema() ||
                    Objects.equals(addColumnChange.getSchemaName(), currentColumn.getRelation().getSchema().getName());
            boolean catalogNameEqual = !control.getIncludeCatalog() ||
                    Objects.equals(addColumnChange.getCatalogName(), currentColumn.getRelation().getSchema().getCatalogName());
            return columnNameEqual && tableNameEqual && schemaNameEqual && catalogNameEqual;
        }
        return false;
    }

}
