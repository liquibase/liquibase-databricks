package liquibase.ext.databricks.change.dropCheckConstraint;


import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DropCheckConstraintGeneratorDatabricks extends AbstractSqlGenerator<DropCheckConstraintStatementDatabricks> {

    @Override
    public boolean supports(DropCheckConstraintStatementDatabricks statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(DropCheckConstraintStatementDatabricks statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("constraintName", statement.getConstraintName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DropCheckConstraintStatementDatabricks statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("ALTER TABLE ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));

        sql.append(" DROP CONSTRAINT ");

        // Constraint Name
        sql.append(statement.getConstraintName());

        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}

