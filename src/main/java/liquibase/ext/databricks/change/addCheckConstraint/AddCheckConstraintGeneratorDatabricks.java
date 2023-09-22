package liquibase.ext.databricks.change.addCheckConstraint;


import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.addCheckConstraint.AddCheckConstraintStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class AddCheckConstraintGeneratorDatabricks extends AbstractSqlGenerator<AddCheckConstraintStatementDatabricks> {

    @Override
    public boolean supports(AddCheckConstraintStatementDatabricks statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(AddCheckConstraintStatementDatabricks statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("constraintName", statement.getConstraintName());
        validationErrors.checkRequiredField("constraintBody", statement.getConstraintBody());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AddCheckConstraintStatementDatabricks statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("ALTER TABLE ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));

        sql.append(" ADD CONSTRAINT ");

        // Constrinat Name
        sql.append(statement.getConstraintName());

        sql.append(" CHECK (");

        sql.append(statement.getConstraintBody().toString() + ")");


        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}

