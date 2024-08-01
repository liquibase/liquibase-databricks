package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.alterTableProperties.AlterTablePropertiesStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class AlterTablePropertiesGeneratorDatabricks extends AbstractSqlGenerator<AlterTablePropertiesStatementDatabricks> {

    @Override
    public boolean supports(AlterTablePropertiesStatementDatabricks statement, Database database) {
        return super.supports(statement, database) && (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(AlterTablePropertiesStatementDatabricks statement, Database database, SqlGeneratorChain<AlterTablePropertiesStatementDatabricks> sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        if (statement.getSetExtendedTableProperties() == null && statement.getUnsetExtendedTableProperties() == null){
            validationErrors.addError("WARNING! Alter Table Properties change require 'setExtendedTableProperties' or 'unsetExtendedTableProperties' element, please add at least one option.");
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AlterTablePropertiesStatementDatabricks statement, Database database, SqlGeneratorChain<AlterTablePropertiesStatementDatabricks> sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("ALTER TABLE ");
        buffer.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));
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
