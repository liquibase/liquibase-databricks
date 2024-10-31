package liquibase.ext.databricks.change.optimizeTable;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class OptimizeTableGenerator extends AbstractSqlGenerator<OptimizeTableStatement> {

    @Override
    public boolean supports(OptimizeTableStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(OptimizeTableStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if zorder columns if null, dont add to sql statement

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(OptimizeTableStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("OPTIMIZE ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));


        if (!statement.getZorderColumns().isEmpty()) {
            sql.append(" ZORDER BY (").append(String.join(", ", statement.getZorderColumns())).append(")");
        }

        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}
