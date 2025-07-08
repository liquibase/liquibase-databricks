package liquibase.ext.databricks.snapshot.jvm;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnSnapshotGeneratorDatabricks extends ColumnSnapshotGenerator {

    private static final String ALL_DATA_TYPES = " BIGINT | BINARY | BOOLEAN | DATE | DECIMAL| DECIMAL\\(| DOUBLE | FLOAT | INT | INTERVAL | VOID | SMALLINT | STRING | VARCHAR\\(\\d+\\) | TIMESTAMP | TIMESTAMP_NTZ | TINYINT | ARRAY<| MAP<| STRUCT<| VARIANT| OBJECT<";
    private static final String DEFAULT_CLAUSE_TERMINATORS = "(?i)(\\s+COMMENT\\s+'| PRIMARY\\s+KEY | FOREIGN\\s+KEY | MASK\\s+\\w+|$|,(\\s+\\w+\\s+" + ALL_DATA_TYPES + ")?|\\)$";
    private static final String GENERATED_BY_DEFAULT_REGEX = "(?i)\\s+GENERATED\\s+(BY\\s+DEFAULT|ALWAYS)\\s+AS\\s+IDENTITY";
    private static final String GENERIC_DEFAULT_VALUE_REGEX = "DEFAULT\\s+(.*?)(" + DEFAULT_CLAUSE_TERMINATORS + "))";
    private static final String SANITIZE_TABLE_SPECIFICATION_REGEX = "(\\(.*?\\))\\s*(?i)(USING|OPTIONS|PARTITIONED BY|CLUSTER BY|LOCATION|TBLPROPERTIES|WITH|$|;$)";
    private static final Pattern DEFAULT_VALUE_PATTERN = Pattern.compile(GENERIC_DEFAULT_VALUE_REGEX);
    private static final Pattern SANITIZE_TABLE_SPECIFICATION_PATTERN = Pattern.compile(SANITIZE_TABLE_SPECIFICATION_REGEX);
    private static final Pattern FUNCTION_PATTERN = Pattern.compile("^(\\w+)\\(.*\\)");

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase) {
            return super.getPriority(objectType, database) + PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{ColumnSnapshotGenerator.class};
    }

    /**
     * Override the default implementation to ARRAY, MAP and STRUCT complex types as
     * Liquibase core does not know how to handle values between <> in the data type.
     */
    @Override
    protected DataType readDataType(CachedRow columnMetadataResultSet, Column column, Database database) throws DatabaseException {
        String dataType = (String) columnMetadataResultSet.get("TYPE_NAME");
        if (dataType != null && database instanceof DatabricksDatabase
                && (dataType.toUpperCase().startsWith("ARRAY")
                || dataType.toUpperCase().startsWith("MAP")
                || dataType.toUpperCase().startsWith("STRUCT"))) {
            DataType type = new DataType(dataType);
            type.setDataTypeId(columnMetadataResultSet.getInt("DATA_TYPE"));
            return type;
        }
        return super.readDataType(columnMetadataResultSet, column, database);
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        //This should work after fix on Databricks side
        if (example instanceof Column) {
            Column column = (Column) super.snapshotObject(example, snapshot);
            //These two are used too often, avoiding them? otherwise there would be too much DB calls
            String showCreateRelatedTableQuery = String.format("SHOW CREATE TABLE %s.%s.%s;",
                    column.getRelation().getSchema().getCatalog(),
                    column.getRelation().getSchema().getName(),
                    column.getRelation().getName());
            if (snapshot.getScratchData(showCreateRelatedTableQuery) != null) {
                String showCreateTableStatement = (String) snapshot.getScratchData(showCreateRelatedTableQuery);
                String defaultValue = extractDefaultValue(showCreateTableStatement, column.getName());
                column.setAutoIncrementInformation(parseAutoIncrementInfo(showCreateTableStatement, column.getName()));
                if (defaultValue != null && !defaultValue.equalsIgnoreCase("null")) {
                    Matcher functionMatcher = FUNCTION_PATTERN.matcher(defaultValue);
                    if (functionMatcher.find()) {
                        DatabaseFunction function = new DatabaseFunction(defaultValue);
                        column.setDefaultValue(function);
                    } else {
                        column.setDefaultValue(defaultValue);
                    }
                }
            }
            return column;
        } else {
            return example;
        }
    }

    private String extractDefaultValue(String createTableStatement, String columnName) {
        String defaultValue = null;
        String sanitizedCreateTableStatement = sanitizeStatement(createTableStatement);
        Pattern columnWithPotentialDefaultPattern = Pattern.compile("[\\(|,]\\s*(" + columnName + "\\s*\\b\\w*\\b.*?)\\s*(?i)(" + ALL_DATA_TYPES + "|( CONSTRAINT |$))");
        Matcher columnWithPotentialDefaultMatcher = columnWithPotentialDefaultPattern.matcher(sanitizedCreateTableStatement);

        String columnWithPotentialDefault = "";
        if (columnWithPotentialDefaultMatcher.find()) {
            columnWithPotentialDefault = columnWithPotentialDefaultMatcher.group(1);
            Matcher stringColumnTypeMatcher = Pattern.compile(columnName + "\\s+(?i)(VARCHAR\\(\\d+\\)|STRING )")
                    .matcher(sanitizedCreateTableStatement);
            Matcher defaultStringValueMatcher = Pattern.compile(columnName + ".+?(?i)DEFAULT\\s+(\\'|\\\")(.*?)\\1")
                    .matcher(sanitizedCreateTableStatement);
            Matcher defaultValueMatcher = DEFAULT_VALUE_PATTERN.matcher(columnWithPotentialDefault);
            if (defaultValueMatcher.find()) {
                defaultValue = defaultValueMatcher.group(1);
                if (stringColumnTypeMatcher.find() && defaultStringValueMatcher.find()
                        && (defaultValue.startsWith("'") || defaultValue.startsWith("\""))) {
                    defaultValue = defaultStringValueMatcher.group(2);
                }
            }
        }
        return defaultValue;
    }

    private String sanitizeStatement(String createTableStatement) {
        createTableStatement = createTableStatement.replace("\n", "");
        String sanitizedCreateTableStatement = createTableStatement.replaceAll(GENERATED_BY_DEFAULT_REGEX, " ");
        Matcher tableSpecificationMatcher = SANITIZE_TABLE_SPECIFICATION_PATTERN.matcher(sanitizedCreateTableStatement);
        if (tableSpecificationMatcher.find()) {
            sanitizedCreateTableStatement = tableSpecificationMatcher.group(1);
        }
        return sanitizedCreateTableStatement;
    }

    private Column.AutoIncrementInformation parseAutoIncrementInfo(String statement, String columnName) {
        String findLineRegex = "(\\b"+columnName+"\\s+\\w+(\\s+NOT\\s+NULL)?\\s+GENERATED\\s+(BY\\s+DEFAULT|ALWAYS)\\s+AS\\s+IDENTITY\\s+\\(START WITH (\\d+) INCREMENT" +
                " BY (\\d+)\\))";
        Matcher findLineMatcher = Pattern.compile(findLineRegex).matcher(statement);
        if (findLineMatcher.find()) {
            if(!findLineMatcher.group(1).startsWith(columnName)){
                return null;
            }
            return getAutoIncrementInformation(findLineMatcher);
        } else {
            return null;
        }
    }

    private static Column.AutoIncrementInformation getAutoIncrementInformation(Matcher findLineMatcher) {
        // GROUP 2 is potential `NOT NULL`, we don't need it.
        // Also Databricks doesn't support syntax like Oracle "GENERATED BY DEFAULT ON NULL AS IDENTITY",
        // so this variant not parsed and autoIncrementInformation.defaultOnNull is not used;
        String generationType = findLineMatcher.group(3);
        int startWith = Integer.parseInt(findLineMatcher.group(4));
        int incrementBy = Integer.parseInt(findLineMatcher.group(5));
        Column.AutoIncrementInformation autoIncrementInformation = new Column.AutoIncrementInformation(startWith, incrementBy);
        autoIncrementInformation.setGenerationType(generationType);
        return autoIncrementInformation;
    }
}
