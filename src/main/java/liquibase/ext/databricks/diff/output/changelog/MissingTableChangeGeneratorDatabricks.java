package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.Change;
import liquibase.change.core.CreateTableChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.MissingTableChangeGenerator;
import liquibase.ext.databricks.change.createTable.CreateTableChangeDatabricks;
import liquibase.ext.databricks.change.createTable.ExtendedTableProperties;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;

public class MissingTableChangeGeneratorDatabricks extends MissingTableChangeGenerator {


    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && Table.class.isAssignableFrom(objectType)) {
            return PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase,
                               ChangeGeneratorChain chain) {
        Change[] changes = super.fixMissing(missingObject, control, referenceDatabase, comparisonDatabase, chain);
        if (changes == null || changes.length == 0) {
            return changes;
        }
        ExtendedTableProperties extendedTableProperties = new ExtendedTableProperties(
                missingObject.getAttribute("Location", String.class),
                missingObject.getAttribute("tblProperties", String.class));

        changes[0] = getCreateTableChangeDatabricks(extendedTableProperties, changes);
        return changes;
    }

    private CreateTableChangeDatabricks getCreateTableChangeDatabricks(ExtendedTableProperties extendedTableProperties, Change[] changes) {
        CreateTableChange temp = (CreateTableChange) changes[0];
        CreateTableChangeDatabricks createTableChangeDatabricks = new CreateTableChangeDatabricks();
        createTableChangeDatabricks.setColumns(temp.getColumns());
        createTableChangeDatabricks.setTableType(temp.getTableType());
        createTableChangeDatabricks.setCatalogName(temp.getCatalogName());
        createTableChangeDatabricks.setSchemaName(temp.getSchemaName());
        createTableChangeDatabricks.setTableName(temp.getTableName());
        createTableChangeDatabricks.setTablespace(temp.getTablespace());
        createTableChangeDatabricks.setRemarks(temp.getRemarks());
        createTableChangeDatabricks.setIfNotExists(temp.getIfNotExists());
        createTableChangeDatabricks.setRowDependencies(temp.getRowDependencies());

        createTableChangeDatabricks.setExtendedTableProperties(extendedTableProperties);
        return createTableChangeDatabricks;
    }

    protected CreateTableChange createCreateTableChange() {
        return new CreateTableChangeDatabricks();
    }
}