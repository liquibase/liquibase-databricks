package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.DoubleType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "double",
        minParameters = 0,
        maxParameters = 0,
        aliases = {"java.sql.Types.DOUBLE", "java.lang.Double"},
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class DoubleDatatypeDatabricks extends DoubleType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            DatabaseDataType type = new DatabaseDataType("DOUBLE", this.getParameters());
            type.setType("DOUBLE");
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