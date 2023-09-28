package liquibase.ext.databricks.snapshot.jvm;


import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.snapshot.jvm.IndexSnapshotGenerator;
import liquibase.structure.DatabaseObject;

/**
 * Databricks does not really support indexes on tables
 * (required customer CLUSTER BY / ZORDER change types to be implemented outside of indexes since they are a part of a Create table / ALTER table DDL/DML
 */
public class IndexSnapshotGeneratorDatabricks extends IndexSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (super.getPriority(objectType, database) > 0 && database instanceof DatabricksDatabase) {
            return DatabricksDatabase.PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] {IndexSnapshotGenerator.class};
    }

    @Override
    public DatabaseObject snapshot(DatabaseObject example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException, InvalidExampleException {
        return chain.snapshot(example, snapshot);
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        return null;
    }

}