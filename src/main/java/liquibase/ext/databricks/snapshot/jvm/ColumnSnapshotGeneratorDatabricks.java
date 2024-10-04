package liquibase.ext.databricks.snapshot.jvm;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

public class ColumnSnapshotGeneratorDatabricks extends ColumnSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase) {
            return super.getPriority(objectType, database) + PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] { ColumnSnapshotGenerator.class };
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
}
