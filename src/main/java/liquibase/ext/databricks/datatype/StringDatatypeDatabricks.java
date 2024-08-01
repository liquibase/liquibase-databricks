package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "string",
        minParameters = 0,
        maxParameters = 0,
        priority = PrioritizedService.PRIORITY_DATABASE,
        aliases = { "varchar", "clob", "java.lang.String" }
)
public class StringDatatypeDatabricks extends LiquibaseDataType {
    public StringDatatypeDatabricks() {
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {

            DatabaseDataType type = new DatabaseDataType("STRING");

            type.setType("STRING");

            return type;
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    @Override
    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.STRING;
    }
}