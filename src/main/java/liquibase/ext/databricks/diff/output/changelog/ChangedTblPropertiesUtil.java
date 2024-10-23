package liquibase.ext.databricks.diff.output.changelog;

import liquibase.diff.Difference;
import liquibase.diff.output.DiffOutputControl;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesChangeDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.ext.databricks.change.alterViewProperties.AlterViewPropertiesChangeDatabricks;
import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.core.Table;
import liquibase.structure.core.View;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for changed table properties diff
 */
public class ChangedTblPropertiesUtil {


    private static final String SPLIT_ON_COMMAS = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"$])";
    private static final String SPLIT_ON_EQUALS = "=(?=(?:[^\"]*\"[^\"]*\")*[^\"$])";

    private ChangedTblPropertiesUtil() {
    }

    /**
     * Get the AlterViewPropertiesChangeDatabricks changes
     */
    static AlterViewPropertiesChangeDatabricks getAlterViewPropertiesChangeDatabricks(View changedObject, DiffOutputControl control, Difference difference) {
        AlterViewPropertiesChangeDatabricks change = (AlterViewPropertiesChangeDatabricks) getAbstractTablePropertiesChangeDatabricks(changedObject, control, difference, AlterViewPropertiesChangeDatabricks.class);
        change.setViewName(changedObject.getName());
        return change;
    }

    /**
     * Get the AlterTablePropertiesChangeDatabricks changes
     */
    static AlterTablePropertiesChangeDatabricks getAlterTablePropertiesChangeDatabricks(Table changedObject, DiffOutputControl control, Difference difference) {
        AlterTablePropertiesChangeDatabricks change = (AlterTablePropertiesChangeDatabricks) getAbstractTablePropertiesChangeDatabricks(changedObject, control, difference, AlterTablePropertiesChangeDatabricks.class);
        change.setTableName(changedObject.getName());
        return change;
    }

    private static AbstractAlterPropertiesChangeDatabricks getAbstractTablePropertiesChangeDatabricks(AbstractDatabaseObject changedObject, DiffOutputControl control, Difference difference, Class<? extends AbstractAlterPropertiesChangeDatabricks> clazz) {
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

        AbstractAlterPropertiesChangeDatabricks change = null;
        try {
            change = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException("Reflection error", e);
        }
        SetExtendedTableProperties setExtendedTableProperties = new SetExtendedTableProperties();
        setExtendedTableProperties.setTblProperties(addPropertiesMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        change.setSetExtendedTableProperties(setExtendedTableProperties);

        UnsetExtendedTableProperties unsetExtendedTableProperties = new UnsetExtendedTableProperties();
        unsetExtendedTableProperties.setTblProperties(String.join(",", removePropertiesMap.keySet()));
        change.setUnsetExtendedTableProperties(unsetExtendedTableProperties);
        setCatalogAndSchema(changedObject, control, change);
        return change;
    }

    private static void setCatalogAndSchema(AbstractDatabaseObject table, DiffOutputControl control, AbstractAlterPropertiesChangeDatabricks change) {
        if (control.getIncludeCatalog()) {
            change.setCatalogName(table.getSchema().getCatalogName());
        }

        if (control.getIncludeSchema()) {
            change.setSchemaName(table.getSchema().getName());
        }
    }
}
