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

/**
 * Created by vesterma on 06/02/14.
 */
public class TableSnapshotGeneratorDatabricks extends TableSnapshotGenerator {

    private final static String LOCATION = "Location";
    private final static String TABLE_PROPERTIES = "Table Properties";
    private final static String DETAILED_TABLE_INFORMATION_NODE = "# Detailed Table Information";

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

        String query = String.format("DESCRIBE TABLE EXTENDED %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), example.getName());
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
            if (detailedInformationNode && tableProperty.get("COL_NAME").equals(TABLE_PROPERTIES)) {
                //TODO should i parse and split TableProperties or keep them all together?
                table.setAttribute(TABLE_PROPERTIES, tableProperty.get("DATA_TYPE"));
            }
            //TODO i can get default value for column here `# Column Default Values` ->"COL_NAME" -> "title", "DATA_TYPE" -> "string", "COMMENT" ->
            // "'title_test'" --actual default value for the column is in comment column of this resultSet
        }
        return table;


//        String query = String.format("SHOW TBLPROPERTIES %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), example.getName());

//        String query = String.format("SHOW TABLE EXTENDED IN %s.%s LIKE '%s';", database.getDefaultCatalogName(), database.getDefaultSchemaName(),
//                example.getName());
//        List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
//                .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
//        for (Map<String, ?> tableProperty : tablePropertiesResponse) {
//            String[] tableParts = ((String) tableProperty.get("INFORMATION")).split("\\r?\\n");
//            for (String tablePart : tableParts) {
//                if (tablePart.startsWith("Location:")) {
//                    table.setAttribute("Location", tablePart.replace("Location: ", ""));
//                }
//                if (tablePart.startsWith("Table Properties:")) {
//                    table.setAttribute("Table Properties", tablePart.replace("Table Properties: [", "").replace("]", ""));
//                }
//            }
//        }

//        String query = String.format("SHOW TBLPROPERTIES %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), example.getName());
//        List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
//                .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
//        for (Map<String, ?> tableProperty : tablePropertiesResponse) {
//            //TODO combine into
//            // "'key0'='value0', 'key1'='value1', .... 'keyN'='valueN'"
//            // csv string
//
//        }
//        return table;
    }

//    @Override
//    public Class<? extends SnapshotGenerator>[] replaces() {
//        return new Class[]{TableSnapshotGenerator.class};
//    }

}