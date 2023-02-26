package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DateTimeType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.change.core.LoadDataChange;

import static liquibase.ext.databricks.database.DatabricksDatabase.DATABRICKS_PRIORITY;

@DataTypeInfo(
        name = "timestamp",
        aliases = {"java.sql.Types.DATETIME", "datetime"},
        minParameters = 0,
        maxParameters = 2,
        priority = DATABRICKS_PRIORITY
)
public class DatetimeDatatypeDatabricks extends LiquibaseDataType {


    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("TIMESTAMP", getParameters());
        }

        return super.toDatabaseDataType(database);
    }

    @Override
    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.DATE;
    }
    public int getPriority() {
        return DATABRICKS_PRIORITY;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }


}
