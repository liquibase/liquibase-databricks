package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.BooleanType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "boolean",
        minParameters = 0,
        maxParameters = 0,
        aliases = {"java.sql.Types.BOOLEAN", "java.lang.Boolean", "bit", "bool"},
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class BooleanDatatypeDatabricks extends BooleanType {

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
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}