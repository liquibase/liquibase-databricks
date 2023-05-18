package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;

@DataTypeInfo(
        name = "timestamp",
        aliases = {"java.sql.Types.DATETIME", "datetime"},
        minParameters = 0,
        maxParameters = 2,
        priority = PRIORITY_DATABASE
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
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }


}
