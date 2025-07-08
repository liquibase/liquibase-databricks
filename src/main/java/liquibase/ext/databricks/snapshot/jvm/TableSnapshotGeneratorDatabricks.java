package liquibase.ext.databricks.snapshot.jvm;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.TableSnapshotGenerator;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableSnapshotGeneratorDatabricks extends TableSnapshotGenerator {

    private static final String LOCATION = "Location";
    private static final String PROVIDER = "Provider";
    private static final String STORAGE_PROPERTIES = "Storage Properties";
    private static final String TABLE_FORMAT = "tableFormat";
    private static final String TBL_PROPERTIES = "tblProperties";
    private static final String CLUSTER_COLUMNS = "clusteringColumns";
    private static final String PARTITION_COLUMNS = "partitionColumns";
    private static final String DETAILED_TABLE_INFORMATION_NODE = "# Detailed Table Information";
    private static final String TABLE_PARTITION_INFORMATION_NODE = "# Partition Information";
    private static final String DATA_TYPE = "DATA_TYPE";
    private static final List<String> FILE_TYPE_PROVIDERS = Arrays.asList("AVRO", "BINARYFILE", "CSV", "JSON", "ORC", "PARQUET", "TEXT");

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase) {
            return super.getPriority(objectType, database) + PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Table table = (Table) super.snapshotObject(example, snapshot);
        Database database = snapshot.getDatabase();
        if (table != null) {
            String query = String.format("DESCRIBE TABLE EXTENDED %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(),
                    example.getName());
            Executor jdbcExecutor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
            List<Map<String, ?>> tablePropertiesResponse = jdbcExecutor.queryForList(new RawParameterizedSqlStatement(query));
            //Skipping changelog tables default values processing
            List<String> changelogTableNames = Arrays.asList(database.getDatabaseChangeLogLockTableName(), database.getDatabaseChangeLogTableName());
            if(!changelogTableNames.contains(table.getName())) {
                String showCreateTableQuery = String.format("SHOW CREATE TABLE %s.%s.%s;", table.getSchema().getCatalog(),
                        table.getSchema().getName(), table.getName());
                if(snapshot.getScratchData(showCreateTableQuery) == null) {
                    String createTableStatement = jdbcExecutor.queryForObject(new RawParameterizedSqlStatement(showCreateTableQuery), String.class);
                    snapshot.setScratchData(showCreateTableQuery, createTableStatement);
                }
            }
            StringBuilder tableFormat = new StringBuilder();
            // DESCRIBE TABLE EXTENDED returns both columns and additional information.
            // We need to make sure "Location" is not column in the table, but table location in s3
            boolean detailedInformationNode = false;
            boolean partitionInformationNode = false;
            StringBuilder partitionColumns = new StringBuilder();
            for (Map<String, ?> tableProperty : tablePropertiesResponse) {
                String currentColName = (String) tableProperty.get("COL_NAME");
                if (currentColName.equals(DETAILED_TABLE_INFORMATION_NODE)) {
                    detailedInformationNode = true;
                    continue;
                }
                if (detailedInformationNode) {
                    if (currentColName.equals(LOCATION)) {
                        table.setAttribute(LOCATION, tableProperty.get(DATA_TYPE));
                    }
                    if (currentColName.equals(PROVIDER) && FILE_TYPE_PROVIDERS.contains(tableProperty.get(DATA_TYPE).toString().toUpperCase())) {
                        tableFormat.append(tableProperty.get(DATA_TYPE));
                    }
                    if (!tableFormat.toString().isEmpty() && currentColName.equals(STORAGE_PROPERTIES)) {
                        if(table.getAttribute(LOCATION, String.class) != null) {
                            tableFormat.append(" ").append(LOCATION.toUpperCase()).append("'").append(table.getAttribute(LOCATION, String.class)).append("' ");
                        }
                        tableFormat.append(extractOptionsFromStorageProperties(tableProperty.get(DATA_TYPE)));
                        table.setAttribute(TABLE_FORMAT, tableFormat.toString());
                    }
                }
                if (currentColName.equals(TABLE_PARTITION_INFORMATION_NODE)) {
                    partitionInformationNode = true;
                    continue;
                }
                if (partitionInformationNode && !currentColName.equals("# col_name")) {
                    if (currentColName.equals("")) {
                        partitionInformationNode = false;
                        continue;
                    }
                    if (partitionColumns.toString().isEmpty()) {
                        partitionColumns.append(currentColName);
                    } else {
                        partitionColumns.append(',').append(currentColName);
                    }
                }
            }
            Map<String, String> tblProperties = getTblPropertiesMap(database, example.getName());
            if (tblProperties.containsKey(CLUSTER_COLUMNS)) {
                table.setAttribute(CLUSTER_COLUMNS, sanitizeClusterColumns(tblProperties.remove(CLUSTER_COLUMNS)));
            }
            if (!partitionColumns.toString().isEmpty()) {
                table.setAttribute(PARTITION_COLUMNS, partitionColumns.toString());
            }
            table.setAttribute(TBL_PROPERTIES, getTblPropertiesString(tblProperties));
        }
        return table;
    }

    private String extractOptionsFromStorageProperties(Object storageProperties) {
        StringBuilder options = new StringBuilder();
        if (storageProperties instanceof String) {
            Matcher matcher = Pattern.compile("(\\b\\w+\\b)=(.*?)(,|\\])").matcher((String) storageProperties);
            if (matcher.find()) {
                options.append(" OPTIONS (").append(matcher.group(1)).append(" '").append(matcher.group(2)).append("'");
                while (matcher.find()) {
                    options.append(", ").append(matcher.group(1)).append(" '").append(matcher.group(2)).append("'");
                }
                options.append(")");
            }
        }
        return options.toString();
    }
    //TODO another way of getting Location is query like
    // select * from `system`.`information_schema`.`tables` where table_name = 'test_table_properties' AND table_schema='liquibase_harness_test_ds';
    // get column 'table_type', if 'EXTERNAL' then
    // Location = get column 'storage_path'
    // cleanup this after approach of getting all properties is settled

    private Map<String, String> getTblPropertiesMap(Database database, String table) throws DatabaseException {
        String query = String.format("SHOW TBLPROPERTIES %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), table);
        List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
        return tablePropertiesResponse.stream()
                .collect(Collectors.toMap(mapElement -> (String) mapElement.get("KEY"), mapElement -> (String) mapElement.get("VALUE")));
    }

    private String getTblPropertiesString(Map<String, String> propertiesMap) {
        StringBuilder csvString = new StringBuilder();
        propertiesMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> csvString.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("', "));
        return csvString.toString().replaceAll(", $", "");
    }

    private String sanitizeClusterColumns(String clusterColumnProperty) {
        Pattern pattern = Pattern.compile("[\\[\\]\\\"]");
        return clusterColumnProperty.replaceAll(pattern.toString(), "");
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{TableSnapshotGenerator.class};
    }

}
