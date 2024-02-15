package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;

@DataTypeInfo(
        name = "double",
        minParameters = 0,
        maxParameters = 0,
        priority = DatabricksDatabase.PRIORITY_DATABASE
)
public class DoubleDatatypeDatabricks extends LiquibaseDataType {

    public DoubleDatatypeDatabricks() {
    }

    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {

            DatabaseDataType type = new DatabaseDataType("DOUBLE", this.getParameters());
            type.setType("DOUBLE");
            return type;
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.NUMERIC;
    }
}