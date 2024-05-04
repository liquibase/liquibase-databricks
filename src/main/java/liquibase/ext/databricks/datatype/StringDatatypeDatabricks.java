package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;

@DataTypeInfo(
        name = "string",
        minParameters = 0,
        maxParameters = 0,
        priority = DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE
)
public class StringDatatypeDatabricks extends LiquibaseDataType {
    public StringDatatypeDatabricks() {
    }

    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

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
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.STRING;
    }
}