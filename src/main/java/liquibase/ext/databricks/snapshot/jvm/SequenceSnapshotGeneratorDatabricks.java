package liquibase.ext.databricks.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.SequenceSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

public class SequenceSnapshotGeneratorDatabricks extends SequenceSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        int priority = super.getPriority(objectType, database);
        if (priority > PRIORITY_NONE && database instanceof DatabricksDatabase) {
            priority += PRIORITY_DATABASE;
        }
        return priority;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{SequenceSnapshotGenerator.class};
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Scope.getCurrentScope().getLog(this.getClass()).info("Sequences are not supported by Databricks");
        return null;
    }


    @Override
    protected SqlStatement getSelectSequenceStatement(Schema schema, Database database) {
        if (database instanceof DatabricksDatabase) {
            // Databricks does not support sequences

            CatalogAndSchema catalogAndSchema = (new CatalogAndSchema(schema.getCatalogName(), schema.getName())).customize(database);

            String jdbcSchemaName = database.correctObjectName(((AbstractJdbcDatabase)database).getJdbcSchemaName(catalogAndSchema), Schema.class);

            String databricksSequenceSql = "SELECT NULL AS SEQUENCE_NAME, NULL AS START_VALUE, NULL AS MIN_VALUE, NULL AS MAX_VALUE, NULL AS INCREMENT_BY, NULL AS WILL_CYCLE " +
                    "FROM " + schema.getCatalogName() + ".information_schema.columns " +
                    "WHERE " +
                    "table_catalog = '" + schema.getCatalogName() + "' " +
                    "AND table_schema = '" + schema.getName() +"' " +
                    "AND 1=0";

            return new RawSqlStatement(databricksSequenceSql);
            }

        return getSelectSequenceStatement(schema, database);
    }
}
