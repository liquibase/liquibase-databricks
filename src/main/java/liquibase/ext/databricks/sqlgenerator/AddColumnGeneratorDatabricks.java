package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.core.AddColumnStatement;

public class AddColumnGeneratorDatabricks extends AddColumnGenerator {

    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        return super.generateSingleColumnSQL(statement, database).replace(" ADD ", " ADD COLUMN ");
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }
}