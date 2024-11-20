package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.AddColumnConfig;
import liquibase.change.Change;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.AddDefaultValueChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.MissingColumnChangeGenerator;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

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
        changes = handleColumnWithDefaultValues((Column) missingObject, control, changes);
        return changes;
    }

    private Change[] handleColumnWithDefaultValues(Column column, DiffOutputControl control, Change[] changes) {
        if(column.getDefaultValue() != null) {
            Optional<AddColumnChange> addColumnOptional = Arrays.stream(changes)
                    .filter(change -> isCurrentColumnChange(change, column, control))
                    .map(AddColumnChange.class::cast).findFirst();
            if(addColumnOptional.isPresent()) {
                AddColumnChange addColumnChange = addColumnOptional.get();
                AddColumnConfig addColumnConfig = addColumnChange.getColumns().get(0);
                String defaultValue = addColumnConfig.getDefaultValue();
                String columnDataType = addColumnConfig.getType();
                Boolean isComputed = addColumnConfig.getComputed();
                if(defaultValue != null) {
                    List<Change> changeList = Arrays.asList(changes);
                    //Filter out add column with default value to split it onto 2 changes
                    changeList = changeList.stream().filter(change -> !isCurrentColumnChange(change, column, control))
                            .collect(Collectors.toList());
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

                    if (isComputed != null && isComputed) {
                        addDefaultValueChange.setDefaultValueComputed(new DatabaseFunction(defaultValue));
                    } else {
                        addDefaultValueChange.setDefaultValue(defaultValue);
                    }
                    addDefaultValueChange.setDefaultValueConstraintName(column.getDefaultValueConstraintName());
                    changeList.add(addColumnChange);
                    changeList.add(addDefaultValueChange);
                    changes = changeList.toArray(new Change[0]);
                }
            }
        }
        return changes;
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
