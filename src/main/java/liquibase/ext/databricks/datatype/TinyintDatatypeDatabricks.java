package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;

@DataTypeInfo(
        name = "tinyint",
        aliases = {"java.sql.Types.TINYINT", "byte"},
        minParameters = 0,
        maxParameters = 0,
        priority = DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE
)
public class TinyintDatatypeDatabricks extends LiquibaseDataType {


    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("TINYINT");
        }

        return super.toDatabaseDataType(database);
    }

    @Override
    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.NUMERIC;
    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }


}
