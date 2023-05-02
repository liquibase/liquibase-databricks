package liquibase.ext.databricks.change.vacuum;


import liquibase.database.Database;
import liquibase.ext.databricks.change.vacuum.VacuumStatement;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class VacuumGenerator extends AbstractSqlGenerator<VacuumStatement> {

    //check support for optimizer operation
    public boolean supports(VacuumStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    public ValidationErrors validate(VacuumStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("catalogName", statement.getCatalogName());
        validationErrors.checkRequiredField("schemaName", statement.getSchemaName());
        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if retentionHours columns if null, dont add to sql statement - just use defaults

        return validationErrors;
    }

    public Sql[] generateSql(VacuumStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("VACUUM ");

        if (statement.getCatalogName().trim().length() >= 1) {
            sql.append(statement.getCatalogName() + ".");
        }

        if (statement.getSchemaName().trim().length() >= 1) {
            sql.append(statement.getSchemaName() + ".");
        }

        if (statement.getTableName().trim().length() >= 1) {
            sql.append(statement.getTableName() + " ");
        }

        if (statement.getRetentionHours() != null) {
            sql.append("RETAIN " + statement.getRetentionHours().toString() + " HOURS;");
        }
        else {
            sql.append(";");
        }
        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}

