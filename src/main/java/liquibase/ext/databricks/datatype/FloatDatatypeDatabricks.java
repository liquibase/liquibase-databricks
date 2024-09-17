package liquibase.ext.databricks.datatype;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.FloatType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;


@DataTypeInfo(
        name = "float",
        minParameters = 0,
        maxParameters = 0,
        aliases = {"java.sql.Types.FLOAT", "java.lang.Float"},
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class FloatDatatypeDatabricks extends FloatType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            DatabaseDataType type = new DatabaseDataType("FLOAT", this.getParameters());
            type.setType("FLOAT");
            return type;
        } else {
            return super.toDatabaseDataType(database);
        }
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}