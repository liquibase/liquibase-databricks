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
import java.util.stream.Stream;

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
    static AbstractAlterPropertiesChangeDatabricks[] getAlterViewPropertiesChangeDatabricks(View changedObject, DiffOutputControl control, Difference difference) {
        AbstractAlterPropertiesChangeDatabricks[] change = getAbstractTablePropertiesChangeDatabricks(changedObject, control, difference, AlterViewPropertiesChangeDatabricks.class);
        Stream.of(change).forEach(c -> ((AlterViewPropertiesChangeDatabricks)c).setViewName(changedObject.getName()));
        return change;
    }

    /**
     * Get the AlterTablePropertiesChangeDatabricks changes
     */
    static AbstractAlterPropertiesChangeDatabricks[] getAlterTablePropertiesChangeDatabricks(Table changedObject, DiffOutputControl control, Difference difference) {
        AbstractAlterPropertiesChangeDatabricks[] change = getAbstractTablePropertiesChangeDatabricks(changedObject, control, difference, AlterTablePropertiesChangeDatabricks.class);
        Stream.of(change).forEach(c -> ((AlterTablePropertiesChangeDatabricks)c).setTableName(changedObject.getName()));
        return change;
    }

    static AbstractAlterPropertiesChangeDatabricks[] getAbstractTablePropertiesChangeDatabricks(AbstractDatabaseObject changedObject, DiffOutputControl control, Difference difference, Class<? extends AbstractAlterPropertiesChangeDatabricks> clazz) {
        AbstractAlterPropertiesChangeDatabricks[] changes = new AbstractAlterPropertiesChangeDatabricks[0];
        Map<String, String> referencedValuesMap = convertToMapExcludingDeltaParameters(difference.getReferenceValue());
        Map<String, String> comparedValuesMap = convertToMapExcludingDeltaParameters(difference.getComparedValue());

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

        if (!addPropertiesMap.isEmpty()) {
            SetExtendedTableProperties setExtendedTableProperties = new SetExtendedTableProperties();
            setExtendedTableProperties.setTblProperties(addPropertiesMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
            AbstractAlterPropertiesChangeDatabricks change = getAbstractAlterPropertiesChangeDatabricks(changedObject, control, clazz);
            change.setSetExtendedTableProperties(setExtendedTableProperties);
            changes = new AbstractAlterPropertiesChangeDatabricks[]{change};
        }

        if (!removePropertiesMap.isEmpty()) {
            UnsetExtendedTableProperties unsetExtendedTableProperties = new UnsetExtendedTableProperties();
            unsetExtendedTableProperties.setTblProperties(String.join(",", removePropertiesMap.keySet()));
            AbstractAlterPropertiesChangeDatabricks change = getAbstractAlterPropertiesChangeDatabricks(changedObject, control, clazz);
            change.setUnsetExtendedTableProperties(unsetExtendedTableProperties);
            if (changes.length == 0) {
                changes = new AbstractAlterPropertiesChangeDatabricks[]{change};
            } else {
                changes = Arrays.copyOf(changes, changes.length + 1);
                changes[changes.length - 1] = change;
            }
        }

        return changes;
    }

    /**
     * Convert the reference value to a map excluding delta parameters
     */
    private static Map<String, String> convertToMapExcludingDeltaParameters(Object referenceValueObject) {
        String referenceValue = referenceValueObject == null ? "" : referenceValueObject.toString();
         return Arrays.stream(referenceValue.split(SPLIT_ON_COMMAS))
                .map(s -> s.split(SPLIT_ON_EQUALS))
                .filter(a -> a.length > 1)
                 .map(a -> new String[]{a[0].trim(), a[1].trim()})
                .filter(a -> !a[0].replace("'", "").matches("^delta.+"))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));
    }

    /**
     * Get the extended properties excluding delta parameters
     */
    public static String getExtendedProperties(String tblProperties) {
        Map<String, String> properties = convertToMapExcludingDeltaParameters(tblProperties);
        return properties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));
    }

    private static AbstractAlterPropertiesChangeDatabricks getAbstractAlterPropertiesChangeDatabricks(AbstractDatabaseObject changedObject, DiffOutputControl control, Class<? extends AbstractAlterPropertiesChangeDatabricks> clazz) {
        AbstractAlterPropertiesChangeDatabricks change;
        try {
            change = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException("Reflection error", e);
        }
        if (control.getIncludeCatalog()) {
            change.setCatalogName(changedObject.getSchema().getCatalogName());
        }

        if (control.getIncludeSchema()) {
            change.setSchemaName(changedObject.getSchema().getName());
        }
        return change;
    }
}
