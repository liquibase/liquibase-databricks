package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.alterViewProperties.AlterViewPropertiesStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class AlterViewPropertiesGeneratorDatabricks extends AbstractSqlGenerator<AlterViewPropertiesStatementDatabricks> {

    @Override
    public boolean supports(AlterViewPropertiesStatementDatabricks statement, Database database) {
        return (database instanceof DatabricksDatabase) && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(AlterViewPropertiesStatementDatabricks statement, Database database, SqlGeneratorChain<AlterViewPropertiesStatementDatabricks> sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        if (statement.getSetExtendedTableProperties() == null && statement.getUnsetExtendedTableProperties() == null){
            validationErrors.addError("WARNING! Alter View Properties change require 'setExtendedTableProperties' or 'unsetExtendedTableProperties' element, please add at least one option.");
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AlterViewPropertiesStatementDatabricks statement, Database database, SqlGeneratorChain<AlterViewPropertiesStatementDatabricks> sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("ALTER VIEW ");
        buffer.append(database.escapeViewName(statement.getCatalogName(), statement.getSchemaName(), statement.getViewName()));
        if (statement.getSetExtendedTableProperties() != null) {
            buffer.append(" SET TBLPROPERTIES (");
            buffer.append(statement.getSetExtendedTableProperties().getTblProperties());
        } else if (statement.getUnsetExtendedTableProperties() != null) {
            buffer.append(" UNSET TBLPROPERTIES (");
            buffer.append(statement.getUnsetExtendedTableProperties().getTblProperties());
        }
        buffer.append(")");

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }
}
