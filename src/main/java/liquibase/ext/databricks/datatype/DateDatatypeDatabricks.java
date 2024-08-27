package liquibase.ext.databricks.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.core.DateType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;

@DataTypeInfo(
        name = "date",
        aliases = {"java.sql.Types.DATE", "java.sql.Date"},
        minParameters = 0,
        maxParameters = 0,
        priority = PrioritizedService.PRIORITY_DATABASE
)
public class DateDatatypeDatabricks extends DateType {

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }


}
