package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RenameViewStatement;
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

}