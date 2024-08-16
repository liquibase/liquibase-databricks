package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.TinyIntType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "tinyint",
        aliases = {"java.sql.Types.TINYINT", "byte"},
        minParameters = 0,
        maxParameters = 0,
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class TinyintDatatypeDatabricks extends TinyIntType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("TINYINT");
        }
        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}
