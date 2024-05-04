package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddAutoIncrementGenerator;
import liquibase.statement.core.AddAutoIncrementStatement;

public class AddAutoIncrementGeneratorDatabricks extends AddAutoIncrementGenerator {

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddAutoIncrementStatement statement, Database database) {
        return super.supports(statement, database)
                && database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(AddAutoIncrementStatement statement,
                                     Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors().addError("Databricks does not support adding AUTO_INCREMENT.");
    }
}
