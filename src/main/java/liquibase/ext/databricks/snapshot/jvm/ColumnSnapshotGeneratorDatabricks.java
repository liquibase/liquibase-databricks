package liquibase.ext.databricks.snapshot.jvm;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.util.List;
import java.util.Map;
public class ColumnSnapshotGeneratorDatabricks extends ColumnSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase)
            return PRIORITY_DATABASE;
        return PRIORITY_NONE;

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
        if (example instanceof Column) {
            Column column = (Column) super.snapshotObject(example, snapshot);
            Database database = snapshot.getDatabase();

            String query = String.format("SELECT column_default from %s.COLUMNS where table_name = '%s' AND table_schema='%s' AND column_name ='%s';",
                    database.getSystemSchema(),
                    column.getRelation().getName(),
                    column.getRelation().getSchema().getName(),
                    column.getName());
            List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                    .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));
            for (Map<String, ?> tableProperty : tablePropertiesResponse) {
                column.setDefaultValue(tableProperty.get("COLUMN_DEFAULT"));
            }
            return column;
        } else {
            return example;
        }
    }

}