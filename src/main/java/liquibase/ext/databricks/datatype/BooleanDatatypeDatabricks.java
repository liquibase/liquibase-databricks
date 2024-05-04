package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "boolean",
        minParameters = 0,
        maxParameters = 0,
        priority = DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE
)
public class BooleanDatatypeDatabricks extends LiquibaseDataType {

    public BooleanDatatypeDatabricks() {
        // empty constructor
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {

            DatabaseDataType type = new DatabaseDataType("BOOLEAN", this.getParameters());
            type.setType("BOOLEAN");
            return type;
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.BOOLEAN;
    }
}