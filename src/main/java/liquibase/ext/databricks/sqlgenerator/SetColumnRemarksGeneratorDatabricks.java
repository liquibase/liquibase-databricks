package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.sqlgenerator.core.SetColumnRemarksGenerator;
import org.apache.commons.lang3.StringUtils;

public class SetColumnRemarksGeneratorDatabricks extends SetColumnRemarksGenerator {

    @Override
    public boolean supports(SetColumnRemarksStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(SetColumnRemarksStatement setColumnRemarksStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", setColumnRemarksStatement.getTableName());
        validationErrors.checkRequiredField("columnName", setColumnRemarksStatement.getColumnName());
        validationErrors.checkDisallowedField("catalogName", setColumnRemarksStatement.getCatalogName(), database, MSSQLDatabase.class);
        if (database instanceof MySQLDatabase) {
            validationErrors.checkRequiredField("columnDataType", StringUtils.trimToNull(setColumnRemarksStatement.getColumnDataType()));
        }
        return validationErrors;
    }

    @Override
    public Warnings warn(SetColumnRemarksStatement statementType, Database database, SqlGeneratorChain<SetColumnRemarksStatement> sqlGeneratorChain) {
        final Warnings warnings = super.warn(statementType, database, sqlGeneratorChain);
        if (database instanceof MySQLDatabase) {
            ((MySQLDatabase) database).warnAboutAlterColumn("setColumnRemarks", warnings);
        }

        return warnings;
    }

    @Override
    public Sql[] generateSql(SetColumnRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        String remarksEscaped = database.escapeStringForDatabase(StringUtils.trimToEmpty(statement.getRemarks()));


        return new Sql[]{new UnparsedSql("ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                    + " ALTER COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()) + " COMMENT '"
                    + remarksEscaped + "'", getAffectedColumn(statement))};

    }

}