package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RenameViewStatement;
import liquibase.structure.core.Relation;
import liquibase.structure.core.View;
import liquibase.sqlgenerator.core.RenameViewGenerator;


public class RenameViewGeneratorDatabricks extends RenameViewGenerator {

    @Override
    public boolean supports(RenameViewStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(RenameViewStatement renameViewStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("oldViewName", renameViewStatement.getOldViewName());
        validationErrors.checkRequiredField("newViewName", renameViewStatement.getNewViewName());

        validationErrors.checkDisallowedField("schemaName", renameViewStatement.getSchemaName(), database, OracleDatabase.class);

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(RenameViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;

        sql = "ALTER VIEW " + database.escapeViewName(statement.getCatalogName(), statement.getSchemaName(), statement.getOldViewName()) + " RENAME TO " + database.escapeObjectName(statement.getNewViewName(), View.class);


        return new Sql[]{
                new UnparsedSql(sql,
                        getAffectedOldView(statement),
                        getAffectedNewView(statement)
                )
        };
    }

    protected Relation getAffectedNewView(RenameViewStatement statement) {
        return new View().setName(statement.getNewViewName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }

    protected Relation getAffectedOldView(RenameViewStatement statement) {
        return new View().setName(statement.getOldViewName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}