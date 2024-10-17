package liquibase.ext.databricks.snapshot.jvm;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.TableSnapshotGenerator;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableSnapshotGeneratorDatabricks extends TableSnapshotGenerator {

    private static final String LOCATION = "Location";
    private static final String TBL_PROPERTIES = "tblProperties";
    private static final String CLUSTER_COLUMNS = "clusteringColumns";
    private static final String DETAILED_TABLE_INFORMATION_NODE = "# Detailed Table Information";

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase)
            return PRIORITY_DATABASE;
        return PRIORITY_NONE;

    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Table table = (Table) super.snapshotObject(example, snapshot);
        Database database = snapshot.getDatabase();
        if (table != null) {
            String query = String.format("DESCRIBE TABLE EXTENDED %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(),
                    example.getName());
            List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                    .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
            // DESCRIBE TABLE EXTENDED returns both columns and additional information.
            // We need to make sure "Location" is not column in the table, but table location in s3
            boolean detailedInformationNode = false;
            for (Map<String, ?> tableProperty : tablePropertiesResponse) {
                if (tableProperty.get("COL_NAME").equals(DETAILED_TABLE_INFORMATION_NODE)) {
                    detailedInformationNode = true;
                    continue;
                }
                if (detailedInformationNode && tableProperty.get("COL_NAME").equals(LOCATION)) {
                    table.setAttribute(LOCATION, tableProperty.get("DATA_TYPE"));
                }
            }
            Map<String, String> tblProperties = getTblPropertiesMap(database, example.getName());
            if (tblProperties.containsKey(CLUSTER_COLUMNS)) {
                // removing clusterColumns, as clusterColumns tblProperty is not allowed in create/alter table statements
                table.setAttribute(CLUSTER_COLUMNS, sanitizeClusterColumns(tblProperties.remove(CLUSTER_COLUMNS)));
            }
            table.setAttribute(TBL_PROPERTIES, getTblPropertiesString(tblProperties));
        }
        return table;
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
                .forEach(entry -> csvString.append(entry.getKey()).append("=").append(entry.getValue()).append(","));
        return csvString.toString().replaceAll(",$", "");
    }

    private String sanitizeClusterColumns(String clusterColumnProperty) {
        Pattern pattern = Pattern.compile("[\\[\\]\\\"]");
        return clusterColumnProperty.replaceAll(pattern.toString(), "");
    }

}