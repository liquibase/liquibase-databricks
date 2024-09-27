package liquibase.ext.databricks.change.alterTableProperties;

import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Setter;

@Setter
@DatabaseChange(name = "alterTableProperties", description = "Alter Table Properties", priority = PrioritizedService.PRIORITY_DATABASE + 500)
public class AlterTablePropertiesChangeDatabricks extends AbstractAlterPropertiesChangeDatabricks {

    private static final String CHANGE_TYPE_SUBJECT = "Table";
    private String tableName;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    protected String getNoPropertiesErrorMessage() {
        return applySubjectToErrorPattern(CHANGE_TYPE_SUBJECT);
    }

    @Override
    public String getConfirmationMessage() {
        return getConfirmationMessage(getTableName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return generateStatements(new AlterTablePropertiesStatementDatabricks(getCatalogName(), getSchemaName(), getTableName()));
    }

    @DatabaseChangeProperty
    public String getTableName() {
        return tableName;
    }
}
