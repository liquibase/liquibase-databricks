package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameColumnGenerator;
import liquibase.statement.core.RenameColumnStatement;
import org.apache.commons.lang3.StringUtils;

public class RenameColumnGeneratorDatabricks extends RenameColumnGenerator {

    @Override
    public boolean supports(RenameColumnStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(RenameColumnStatement renameColumnStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", renameColumnStatement.getTableName());
        validationErrors.checkRequiredField("oldColumnName", renameColumnStatement.getOldColumnName());
        validationErrors.checkRequiredField("newColumnName", renameColumnStatement.getNewColumnName());

        if (database instanceof MySQLDatabase) {
            validationErrors.checkRequiredField("columnDataType", StringUtils.trimToNull(renameColumnStatement.getColumnDataType()));
        }

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(RenameColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;

        sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " RENAME COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getOldColumnName()) + " TO " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getNewColumnName());

        return new Sql[] {
                new UnparsedSql(sql, getAffectedOldColumn(statement), getAffectedNewColumn(statement))
        };
    }

}