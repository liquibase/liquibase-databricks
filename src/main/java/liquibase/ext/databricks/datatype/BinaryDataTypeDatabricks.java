package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.BlobType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(name = "binary", aliases = {"longblob", "java.sql.Types.BLOB",
       "java.sql.Types.BINARY", "tinyblob", "mediumblob"}, minParameters = 0, maxParameters = 0
        , priority = PrioritizedService.PRIORITY_DATABASE)
public class BinaryDataTypeDatabricks extends BlobType {


    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("BINARY");
        }

        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}
