package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.Change;
import liquibase.database.Database;
import liquibase.diff.Difference;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.ChangedTableChangeGenerator;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Custom diff change generator for Databricks
 */
public class ChangedTableChangeGeneratorDatabricks extends ChangedTableChangeGenerator {

    private static final String SPLIT_ON_COMMAS = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"$])";
    private static final String SPLIT_ON_EQUALS = "=(?=(?:[^\"]*\"[^\"]*\")*[^\"$])";

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && super.getPriority(objectType, database) > PRIORITY_NONE) {
            return PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Change[] fixChanged(DatabaseObject changedObject, ObjectDifferences differences, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        Change[] changes = super.fixChanged(changedObject, differences, control, referenceDatabase, comparisonDatabase, chain);
        for (Difference difference : differences.getDifferences()) {
            if (difference.getField().equals("tblProperties")) {
                AlterTablePropertiesChangeDatabricks change = getAlterTablePropertiesChangeDatabricks((Table) changedObject, control, difference);

                if (changes == null || changes.length == 0) {
                    changes = new Change[] {change};
                } else {
                    changes = Arrays.copyOf(changes, changes.length + 1);
                    changes[changes.length - 1] = change;
                }
            }
        }
        return changes;
    }

    /**
     * Get the AlterTablePropertiesChangeDatabricks changes
     */
    private AlterTablePropertiesChangeDatabricks getAlterTablePropertiesChangeDatabricks(Table changedObject, DiffOutputControl control, Difference difference) {
        String referenceValue = difference.getReferenceValue() == null ? "" : difference.getReferenceValue().toString();
        Map<String, String> referencedValuesMap = Arrays.stream(referenceValue.split(SPLIT_ON_COMMAS))
                .map(s -> s.split(SPLIT_ON_EQUALS))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));

        String comparedValue = difference.getComparedValue() == null ? "" : difference.getComparedValue().toString();

        Map<String, String> comparedValuesMap = Arrays.stream(comparedValue.split(SPLIT_ON_COMMAS))
                .map(s -> s.split(SPLIT_ON_EQUALS))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));

        Map<String, String> addPropertiesMap = new HashMap<>();
        //first we add the missing or changed properties
        referencedValuesMap.forEach((key, value) -> {
            if (!comparedValuesMap.containsKey(key) || !comparedValuesMap.get(key).equals(value)) {
                addPropertiesMap.put(key, value);
            }
        });
        //then we remove the properties that are not in the reference
        Map<String, String> removePropertiesMap = comparedValuesMap.entrySet().stream()
                .filter(entry -> !referencedValuesMap.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        AlterTablePropertiesChangeDatabricks change = new AlterTablePropertiesChangeDatabricks();
        SetExtendedTableProperties setExtendedTableProperties = new SetExtendedTableProperties();
        setExtendedTableProperties.setTblProperties(addPropertiesMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        change.setSetExtendedTableProperties(setExtendedTableProperties);

        UnsetExtendedTableProperties unsetExtendedTableProperties = new UnsetExtendedTableProperties();
        unsetExtendedTableProperties.setTblProperties(String.join(",", removePropertiesMap.keySet()));
        change.setUnsetExtendedTableProperties(unsetExtendedTableProperties);
        setTableProperties(changedObject, control, change);
        return change;
    }

    private void setTableProperties(Table table, DiffOutputControl control, AlterTablePropertiesChangeDatabricks change) {
        change.setTableName(table.getName());
        if (control.getIncludeCatalog()) {
            change.setCatalogName(table.getSchema().getCatalogName());
        }

        if (control.getIncludeSchema()) {
            change.setSchemaName(table.getSchema().getName());
        }
    }

}
