package liquibase.ext.databricks.datatype;

import liquibase.datatype.core.BigIntType;
import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;


@DataTypeInfo(name = "bigint", aliases = {"java.sql.Types.BIGINT", "java.math.BigInteger", "java.lang.Long", "integer8", "bigserial", "serial8", "int8"},
        minParameters = 0, maxParameters = 0, priority = PrioritizedService.PRIORITY_DATABASE)
public class BigintDatatypeDatabricks extends BigIntType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof DatabricksDatabase) {
            return new DatabaseDataType("BIGINT");
        }
        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}