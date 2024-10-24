package liquibase.ext.databricks.change.alterViewProperties;

import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.ext.databricks.change.AbstractAlterPropertiesChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Setter;

@Setter
@DatabaseChange(name = "alterViewProperties", description = "Alter View Properties", priority = PrioritizedService.PRIORITY_DATABASE)
public class AlterViewPropertiesChangeDatabricks extends AbstractAlterPropertiesChangeDatabricks {

    private static final String CHANGE_TYPE_SUBJECT = "View";
    private String viewName;

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
        return getConfirmationMessage(getViewName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return generateStatements(new AlterViewPropertiesStatementDatabricks(getCatalogName(), getSchemaName(), getViewName()));
    }

    @DatabaseChangeProperty
    public String getViewName() {
        return viewName;
    }
}
