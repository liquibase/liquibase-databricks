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

public class TableSnapshotGeneratorDatabricks extends TableSnapshotGenerator {

    private static final String LOCATION = "Location";
    private static final String TABLE_PROPERTIES = "Table Properties";
    private static final String TBL_PROPERTIES = "tblProperties";
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
                if (detailedInformationNode && tableProperty.get("COL_NAME").equals(TABLE_PROPERTIES)) {
                    String tblProperties = (String) tableProperty.get("DATA_TYPE");
                    table.setAttribute(TBL_PROPERTIES, tblProperties.substring(1, tblProperties.length() - 1));// remove starting and ending square brackets
                }
            }
        }
        return table;

//        String query = String.format("SHOW TBLPROPERTIES %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), example.getName());
//        List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
//                .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
//        for (Map<String, ?> tableProperty : tablePropertiesResponse) {
//            table.setAttribute((String) tableProperty.get("KEY"), tableProperty.get("VALUE"));
//        }
//        return table;
    }

//    @Override
//    public Class<? extends SnapshotGenerator>[] replaces() {
//        return new Class[]{TableSnapshotGenerator.class};
//    }

}