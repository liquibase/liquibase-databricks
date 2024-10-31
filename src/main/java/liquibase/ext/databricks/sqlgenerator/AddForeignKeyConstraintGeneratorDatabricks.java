package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.AddForeignKeyConstraintStatement;
import liquibase.sqlgenerator.core.AddForeignKeyConstraintGenerator;

public class AddForeignKeyConstraintGeneratorDatabricks extends AddForeignKeyConstraintGenerator {

    @Override
    @SuppressWarnings({"SimplifiableIfStatement"})
    public boolean supports(AddForeignKeyConstraintStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(AddForeignKeyConstraintStatement addForeignKeyConstraintStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();

        if ((addForeignKeyConstraintStatement.isInitiallyDeferred() || addForeignKeyConstraintStatement.isDeferrable()) && !database.supportsInitiallyDeferrableColumns()) {
            validationErrors.checkDisallowedField("initiallyDeferred", addForeignKeyConstraintStatement.isInitiallyDeferred(), database);
            validationErrors.checkDisallowedField("deferrable", addForeignKeyConstraintStatement.isDeferrable(), database);
        }

        validationErrors.checkRequiredField("baseColumnNames", addForeignKeyConstraintStatement.getBaseColumnNames());
        validationErrors.checkRequiredField("baseTableNames", addForeignKeyConstraintStatement.getBaseTableName());
        validationErrors.checkRequiredField("referencedColumnNames", addForeignKeyConstraintStatement.getReferencedColumnNames());
        validationErrors.checkRequiredField("referencedTableName", addForeignKeyConstraintStatement.getReferencedTableName());
        validationErrors.checkRequiredField("constraintName", addForeignKeyConstraintStatement.getConstraintName());

        validationErrors.checkDisallowedField("onDelete", addForeignKeyConstraintStatement.getOnDelete(), database, SybaseDatabase.class);

        if (database instanceof SybaseASADatabase) {
            validationErrors.addWarning("SQL Anywhere will apply RESTRICT instead of NO ACTION.");
        }

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AddForeignKeyConstraintStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ")
                .append(database.escapeTableName(statement.getBaseTableCatalogName(), statement.getBaseTableSchemaName(), statement.getBaseTableName()))
                .append(" ADD CONSTRAINT ");
        if (!(database instanceof InformixDatabase)) {
            sb.append(database.escapeConstraintName(statement.getConstraintName()));
        }
        sb.append(" FOREIGN KEY (")
                .append(database.escapeColumnNameList(statement.getBaseColumnNames()))
                .append(") REFERENCES ")
                .append(database.escapeTableName(statement.getReferencedTableCatalogName(), statement.getReferencedTableSchemaName(), statement.getReferencedTableName()))
                .append(" (")
                .append(database.escapeColumnNameList(statement.getReferencedColumnNames()))
                .append(")");



        return new Sql[]{
                new UnparsedSql(sb.toString(), getAffectedForeignKey(statement))
        };
    }

}
