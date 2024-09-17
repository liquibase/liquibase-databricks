package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddPrimaryKeyGenerator;
import liquibase.statement.core.AddPrimaryKeyStatement;

public class AddPrimaryKeyGeneratorDatabricks extends AddPrimaryKeyGenerator {

    @Override
    public boolean supports(AddPrimaryKeyStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(AddPrimaryKeyStatement addPrimaryKeyStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("columnNames", addPrimaryKeyStatement.getColumnNames());
        validationErrors.checkRequiredField("tableName", addPrimaryKeyStatement.getTableName());

        if (addPrimaryKeyStatement.isClustered() != null) {
            if (database instanceof PostgresDatabase) {
                if (addPrimaryKeyStatement.isClustered() && addPrimaryKeyStatement.getConstraintName() == null) {
                    validationErrors.addError("Postgresql requires constraintName on addPrimaryKey when clustered=true");
                }
            } else if (database instanceof MSSQLDatabase || database instanceof MockDatabase) {
                //clustered is fine
            } else if (addPrimaryKeyStatement.isClustered()) {
                validationErrors.addError("Cannot specify clustered=true on "+database.getShortName());
            }
        }

        if (!((database instanceof OracleDatabase) || (database instanceof AbstractDb2Database))) {
            validationErrors.checkDisallowedField("forIndexName", addPrimaryKeyStatement.getForIndexName(), database);
        }

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AddPrimaryKeyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;
        sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " ADD CONSTRAINT " + database.escapeConstraintName(statement.getConstraintName()) + " PRIMARY KEY";
        sql += " (" + database.escapeColumnNameList(statement.getColumnNames()) + ")";


        return new Sql[]{
                new UnparsedSql(sql, getAffectedPrimaryKey(statement))
        };
    }

}