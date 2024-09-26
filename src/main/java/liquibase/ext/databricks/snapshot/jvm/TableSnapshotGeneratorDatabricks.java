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
        for (Map<String, ?> tableProperty : tablePropertiesResponse) {
            table.setAttribute((String) tableProperty.get("KEY"), tableProperty.get("VALUE"));
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