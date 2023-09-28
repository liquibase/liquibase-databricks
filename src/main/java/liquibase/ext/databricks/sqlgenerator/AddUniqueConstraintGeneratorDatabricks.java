package liquibase.ext.databricks.sqlgenerator;


import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddUniqueConstraintGenerator;
import liquibase.statement.core.AddUniqueConstraintStatement;

public class AddUniqueConstraintGeneratorDatabricks extends AddUniqueConstraintGenerator {

    @Override
    public int getPriority() {
        return DatabricksDatabase.PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddUniqueConstraintStatement statement, Database database) {
        return super.supports(statement, database)
                && database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(AddUniqueConstraintStatement statement,
                                     Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors().addError(
                "Databricks does not support altering unique constraint key.");
    }
}