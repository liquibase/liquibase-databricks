package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;


@DataTypeInfo(
        name = "float",
        minParameters = 0,
        maxParameters = 0,
        priority = DatabricksDatabase.PRIORITY_DATABASE
)
public class FloatDatatypeDatabricks extends LiquibaseDataType {
    public FloatDatatypeDatabricks() {
    }

    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {

            DatabaseDataType type = new DatabaseDataType("FLOAT", this.getParameters());
            type.setType("FLOAT");
            return type;
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.NUMERIC;
    }
}