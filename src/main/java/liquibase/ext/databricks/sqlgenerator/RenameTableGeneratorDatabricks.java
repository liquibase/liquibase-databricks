package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.sqlgenerator.core.RenameTableGenerator;

public class RenameTableGeneratorDatabricks extends RenameTableGenerator {

    @Override
    public boolean supports(RenameTableStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public ValidationErrors validate(RenameTableStatement renameTableStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("newTableName", renameTableStatement.getNewTableName());
        validationErrors.checkRequiredField("oldTableName", renameTableStatement.getOldTableName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(RenameTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;

        sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getOldTableName()) + " RENAME TO " + database.escapeObjectName(statement.getNewTableName(), Table.class);

        return new Sql[]{
                new UnparsedSql(sql,
                        getAffectedOldTable(statement),
                        getAffectedNewTable(statement)
                )
        };
    }

    protected Relation getAffectedNewTable(RenameTableStatement statement) {
        return new Table().setName(statement.getNewTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }

    protected Relation getAffectedOldTable(RenameTableStatement statement) {
        return new Table().setName(statement.getOldTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}