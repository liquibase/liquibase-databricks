package liquibase.ext.databricks.change.alterViewProperties;

import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static liquibase.statement.SqlStatement.EMPTY_SQL_STATEMENT;

@Setter
@DatabaseChange(name = "alterViewProperties", description = "Alter View Properties", priority = PrioritizedService.PRIORITY_DATABASE + 500)
public class AlterViewPropertiesChangeDatabricks extends AbstractChange {

    private String viewName;
    private String catalogName;
    private String schemaName;
    private SetExtendedTableProperties setExtendedTableProperties;
    private UnsetExtendedTableProperties unsetExtendedTableProperties;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (setExtendedTableProperties == null && unsetExtendedTableProperties == null) {
            validationErrors.addError("Alter View Properties change require 'setExtendedTableProperties' or 'unsetExtendedTableProperties' element, please add at least one option.");
        }
        return validationErrors;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully altered.", getCatalogName(), getSchemaName(), getViewName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        AlterViewPropertiesStatementDatabricks statement = new AlterViewPropertiesStatementDatabricks(getCatalogName(), getSchemaName(), getViewName());

        if (setExtendedTableProperties != null) {
            statement.setSetExtendedTableProperties(setExtendedTableProperties);
        } else if (unsetExtendedTableProperties != null) {
            statement.setUnsetExtendedTableProperties(unsetExtendedTableProperties);
        }

        List<SqlStatement> statements = new ArrayList<>();
        statements.add(statement);
        return statements.toArray(EMPTY_SQL_STATEMENT);
    }

    @DatabaseChangeProperty
    public String getCatalogName() {
        return catalogName;
    }

    @DatabaseChangeProperty
    public String getSchemaName() {
        return schemaName;
    }

    @DatabaseChangeProperty
    public String getViewName() {
        return viewName;
    }

    @DatabaseChangeProperty
    public SetExtendedTableProperties getSetExtendedTableProperties() {
        return setExtendedTableProperties;
    }

    @DatabaseChangeProperty
    public UnsetExtendedTableProperties getUnsetExtendedTableProperties() {
        return unsetExtendedTableProperties;
    }
}
