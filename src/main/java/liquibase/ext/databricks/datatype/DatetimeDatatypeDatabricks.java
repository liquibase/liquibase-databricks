package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.DateTimeType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "timestamp",
        aliases = {"java.sql.Types.DATETIME", "datetime"},
        minParameters = 0,
        maxParameters = 0,
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class DatetimeDatatypeDatabricks extends DateTimeType {


    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("TIMESTAMP");
        }
        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}
