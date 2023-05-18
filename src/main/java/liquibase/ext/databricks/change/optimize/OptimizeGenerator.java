package liquibase.ext.databricks.change.optimize;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class OptimizeGenerator extends AbstractSqlGenerator<OptimizeStatement> {

    @Override
    //check support for optimizer operation
    public boolean supports(OptimizeStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(OptimizeStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if zorder columns if null, dont add to sql statement

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(OptimizeStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("OPTIMIZE ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));


        if (!statement.getZorderColumns().isEmpty()) {
            sql.append(" ZORDER BY  (" + String.join(", ", statement.getZorderColumns()) + ")");
        }

        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}
