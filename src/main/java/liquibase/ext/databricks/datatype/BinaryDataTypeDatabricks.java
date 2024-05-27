package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BlobType;
import liquibase.ext.databricks.database.DatabricksDatabase;


public class BinaryDataTypeDatabricks extends BlobType {


    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("BINARY");
        }

        return super.toDatabaseDataType(database);
    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}
