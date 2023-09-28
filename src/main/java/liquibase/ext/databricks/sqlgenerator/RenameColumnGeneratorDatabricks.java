package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RenameColumnStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import liquibase.util.StringUtil;
import liquibase.sqlgenerator.core.RenameColumnGenerator;

public class RenameColumnGeneratorDatabricks extends RenameColumnGenerator {

    @Override
    public boolean supports(RenameColumnStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public ValidationErrors validate(RenameColumnStatement renameColumnStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", renameColumnStatement.getTableName());
        validationErrors.checkRequiredField("oldColumnName", renameColumnStatement.getOldColumnName());
        validationErrors.checkRequiredField("newColumnName", renameColumnStatement.getNewColumnName());

        if (database instanceof MySQLDatabase) {
            validationErrors.checkRequiredField("columnDataType", StringUtil.trimToNull(renameColumnStatement.getColumnDataType()));
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

    protected Column getAffectedOldColumn(RenameColumnStatement statement) {
        return new Column().setName(statement.getOldColumnName()).setRelation(new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName()));
    }

    protected Column getAffectedNewColumn(RenameColumnStatement statement) {
        return new Column().setName(statement.getNewColumnName()).setRelation(new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName()));
    }
}