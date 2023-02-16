package liquibase.ext.databricks.change.optimize;

import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class OptimizeGenerator extends AbstractSqlGenerator<OptimizeStatement> {

    //check support for optimizer operation
    public boolean supports(OptimizeStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    public ValidationErrors validate(OptimizeStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("catalogName", statement.getCatalogName());
        validationErrors.checkRequiredField("schemaName", statement.getSchemaName());
        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if zorder columns if null, dont add to sql statement

        return validationErrors;
    }

    public Sql[] generateSql(OptimizeStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("OPTIMIZE ");

        if (statement.getCatalogName().trim().length() >= 1) {
            sql.append(statement.getCatalogName() + ".");
        }

        if (statement.getSchemaName().trim().length() >= 1) {
            sql.append(statement.getSchemaName() + ".");
        }

        if (statement.getTableName().trim().length() >= 1) {
            sql.append(statement.getTableName() + " ");
        }

        if (statement.getZorderColumns().size() >= 1) {
            sql.append("ZORDER BY  (" + String.join(", ", statement.getZorderColumns()) + ");");
        }
        else {
            sql.append(";");
        }
        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}
