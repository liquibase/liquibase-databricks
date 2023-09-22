package liquibase.ext.databricks.change.vacuumTable;


import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class VacuumTableGenerator extends AbstractSqlGenerator<VacuumTableStatement> {

    //check support for optimizer operation
    @Override
    public boolean supports(VacuumTableStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(VacuumTableStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if retentionHours columns if null, dont add to sql statement - just use defaults

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(VacuumTableStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("VACUUM ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));



        if (statement.getRetentionHours() != null) {
            sql.append(" RETAIN " + statement.getRetentionHours().toString() + " HOURS ");
        }

        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}

