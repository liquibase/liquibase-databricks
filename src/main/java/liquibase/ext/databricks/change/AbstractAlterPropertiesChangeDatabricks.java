package liquibase.ext.databricks.change;

import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.ext.databricks.change.alterViewProperties.AlterViewPropertiesStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.SqlStatement;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static liquibase.statement.SqlStatement.EMPTY_SQL_STATEMENT;

@Setter
public abstract class AbstractAlterPropertiesChangeDatabricks extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private SetExtendedTableProperties setExtendedTableProperties;
    private UnsetExtendedTableProperties unsetExtendedTableProperties;
    private final String elementName;

    public AbstractAlterPropertiesChangeDatabricks (String elementName) {
        this.elementName = elementName;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (setExtendedTableProperties == null && unsetExtendedTableProperties == null) {
            validationErrors.addError(getNoPropertiesErrorMessage());
        }
        return validationErrors;
    }

    protected abstract String getNoPropertiesErrorMessage();

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully altered.", getCatalogName(), getSchemaName(), getElementName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        AlterViewPropertiesStatementDatabricks statement = new AlterViewPropertiesStatementDatabricks(getCatalogName(), getSchemaName(), getElementName());

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

    public String getElementName() {
        return elementName;
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
