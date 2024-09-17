package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.IntType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import lombok.Getter;
import lombok.Setter;


@DataTypeInfo(
        name = "int",
        minParameters = 0,
        maxParameters = 0,
        aliases = {"integer", "java.sql.Types.INTEGER", "java.lang.Integer"},
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class IntegerDatatypeDatabricks extends IntType {

    @Getter
    @Setter
    private boolean autoIncrement;

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            DatabaseDataType type = new DatabaseDataType("INT", this.getParameters());
            type.setType("INT");
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