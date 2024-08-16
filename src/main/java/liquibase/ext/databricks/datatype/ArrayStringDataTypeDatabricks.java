package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(name = "array<string>", minParameters = 0, maxParameters = 0, priority = PrioritizedService.PRIORITY_DATABASE)
public class ArrayStringDataTypeDatabricks extends LiquibaseDataType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("ARARY<STRING>");
        }
        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.STRING;
    }
}