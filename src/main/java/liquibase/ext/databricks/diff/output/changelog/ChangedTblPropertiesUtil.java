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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for changed table properties diff
 */
public class ChangedTblPropertiesUtil {


    /**
     * Delta properties that are volatile and automatically maintained by Delta.
     * These properties should be excluded from diff operations as they change frequently
     * and are not user-controlled.
     */
    private static final Set<String> NON_ALLOWED_DELTA_PROPERTIES = Stream.of(
            "delta.rowTracking.materializedRowCommitVersionColumnName",
            "delta.rowTracking.materializedRowIdColumnName", 
            "delta.columnMapping.maxColumnId",
            "delta.feature.clustering"
    ).collect(Collectors.toSet());

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
        Map<String, String> referencedValuesMap = convertToMap(difference.getReferenceValue());
        Map<String, String> comparedValuesMap = convertToMap(difference.getComparedValue());

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
            setExtendedTableProperties.setTblProperties(reconstructPropertiesString(addPropertiesMap));
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
     * Convert the reference value to a map
     */
    private static Map<String, String> convertToMap(Object referenceValueObject) {
        String referenceValue = referenceValueObject == null ? "" : referenceValueObject.toString();
        if (referenceValue.trim().isEmpty()) {
            return new HashMap<>();
        }
        
        Map<String, String> result = new HashMap<>();
        
        // Use a more sophisticated approach: find all key=value pairs
        // by looking for patterns where we have key= followed by value
        int i = 0;
        
        while (i < referenceValue.length()) {
            // Skip leading whitespace and commas
            while (i < referenceValue.length() && (Character.isWhitespace(referenceValue.charAt(i)) || referenceValue.charAt(i) == ',')) {
                i++;
            }
            
            if (i >= referenceValue.length()) {
                break;
            }
            
            // Find the next key=value pair
            KeyValuePair pair = extractNextKeyValuePair(referenceValue, i);
            if (pair == null) {
                break;
            }
            
            // Filter out blocked delta properties
            if (!isBlockedDeltaProperty(pair.key)) {
                result.put(pair.key, pair.value);
            }
            i = pair.nextIndex;
        }
        
        return result;
    }
    
    private static class KeyValuePair {
        String key;
        String value;
        int nextIndex;
        
        KeyValuePair(String key, String value, int nextIndex) {
            this.key = key;
            this.value = value;
            this.nextIndex = nextIndex;
        }
    }
    
    /**
     * Extract the next key=value pair starting from the given position.
     * This method looks ahead to find the next property boundary.
     */
    private static KeyValuePair extractNextKeyValuePair(String input, int startIndex) {
        if (startIndex >= input.length()) {
            return null;
        }
        
        // Find the equals sign for this key
        int equalsIndex = input.indexOf('=', startIndex);
        if (equalsIndex == -1) {
            return null;
        }
        
        String key = input.substring(startIndex, equalsIndex).trim();
        int valueStart = equalsIndex + 1;
        
        // Now find where this value ends by looking for the next key=value pattern
        int valueEnd = findValueEnd(input, valueStart);
        
        String value = input.substring(valueStart, valueEnd).trim();
        
        return new KeyValuePair(key, value, valueEnd);
    }
    
    /**
     * Find where a property value ends by looking for the next key=value pattern.
     * This handles complex values that contain commas, brackets, etc.
     */
    private static int findValueEnd(String input, int valueStart) {
        // Look for the next property by finding patterns like ",key=" or end of string
        int i = valueStart;
        int bracketDepth = 0;
        boolean inQuotes = false;
        char quoteChar = 0;
        
        while (i < input.length()) {
            char c = input.charAt(i);
            
            if (!inQuotes) {
                if (c == '"' || c == '\'') {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                } else if (c == ',' && bracketDepth == 0) {
                    // Found a comma at top level - check if this starts a new property
                    int nextPropertyStart = i + 1;
                    
                    // Skip whitespace after comma
                    while (nextPropertyStart < input.length() && Character.isWhitespace(input.charAt(nextPropertyStart))) {
                        nextPropertyStart++;
                    }
                    
                    // Look for the pattern "key=" after the comma
                    int nextEquals = input.indexOf('=', nextPropertyStart);
                    if (nextEquals != -1) {
                        // Check if there's a valid key between comma and equals
                        String potentialKey = input.substring(nextPropertyStart, nextEquals).trim();
                        if (isValidPropertyKey(potentialKey)) {
                            // This comma starts a new property
                            return i;
                        }
                    }
                }
            } else {
                if (c == quoteChar) {
                    // Check if it's escaped
                    if (i == 0 || input.charAt(i - 1) != '\\') {
                        inQuotes = false;
                    }
                }
            }
            
            i++;
        }
        
        return input.length(); // End of string
    }
    
    /**
     * Check if a string looks like a valid property key.
     * Property keys typically contain letters, numbers, dots, underscores, and may be quoted.
     */
    private static boolean isValidPropertyKey(String key) {
        if (key.isEmpty()) {
            return false;
        }
        
        // Remove quotes if present
        String cleanKey = key;
        if ((key.startsWith("'") && key.endsWith("'")) || (key.startsWith("\"") && key.endsWith("\""))) {
            cleanKey = key.substring(1, key.length() - 1);
        }
        
        // A valid key should not contain commas, equals signs, or brackets (unless quoted)
        // and should contain at least one letter or dot (property-like)
        return cleanKey.matches("[a-zA-Z0-9._-]+") || key.startsWith("'") || key.startsWith("\"");
    }
    
    /**
     * Check if a property key should be blocked from diff operations.
     * Blocked properties are volatile delta properties that are automatically maintained.
     */
    private static boolean isBlockedDeltaProperty(String key) {
        if (key == null) {
            return false;
        }
        
        // Remove quotes if present to get the clean property name
        String cleanKey = key;
        if ((key.startsWith("'") && key.endsWith("'")) || (key.startsWith("\"") && key.endsWith("\""))) {
            cleanKey = key.substring(1, key.length() - 1);
        }
        
        return NON_ALLOWED_DELTA_PROPERTIES.contains(cleanKey);
    }

    /**
     * Get the extended properties excluding blocked delta properties
     */
    public static String getFilteredTblProperties(String tblProperties) {
        Map<String, String> properties = convertToMap(tblProperties);
        return properties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));
    }

    /**
     * Parse a properties string into a Map using robust parsing logic.
     * This method handles complex property values that contain commas, brackets, etc.
     */
    public static Map<String, String> parsePropertiesString(String propertiesString) {
        return convertToMap(propertiesString);
    }

    /**
     * Reconstruct a properties string from a map of key-value pairs.
     * This method is more robust than simple comma joining because it ensures
     * that the resulting string can be parsed back correctly by the convertToMap method.
     */
    private static String reconstructPropertiesString(Map<String, String> propertiesMap) {
        if (propertiesMap.isEmpty()) {
            return "";
        }
        
        // For now, we can use simple comma joining since our parser is robust enough
        // to handle complex values. However, this method provides a place to implement
        // more sophisticated reconstruction logic if needed in the future.
        return propertiesMap.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
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
