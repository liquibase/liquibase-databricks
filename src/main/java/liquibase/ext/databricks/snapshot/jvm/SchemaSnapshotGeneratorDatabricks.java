package liquibase.ext.databricks.snapshot.jvm;


import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.SchemaSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.util.JdbcUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchemaSnapshotGeneratorDatabricks extends SchemaSnapshotGenerator {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase) {
            return super.getPriority(objectType, database) + PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    protected String[] getDatabaseSchemaNames(Database database) throws SQLException, DatabaseException {
        List<String> returnList = new ArrayList<>();

        try (ResultSet schemas = ((JdbcConnection) database.getConnection()).getMetaData().getSchemas(database
                .getDefaultCatalogName(), null)) {
            while (schemas.next()) {
                returnList.add(JdbcUtil.getValueForColumn(schemas, "TABLE_SCHEM", database));
            }
        }

        return returnList.toArray(new String[0]);
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{SchemaSnapshotGenerator.class};
    }

}