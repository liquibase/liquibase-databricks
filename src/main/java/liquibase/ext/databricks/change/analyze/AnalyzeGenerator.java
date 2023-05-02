package liquibase.ext.databricks.change.analyze;


import liquibase.database.Database;
import liquibase.ext.databricks.change.analyze.AnalyzeStatement;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import java.util.Map;
import java.util.Optional;

public class AnalyzeGenerator extends AbstractSqlGenerator<AnalyzeStatement> {

    //check support for optimizer operation
    public boolean supports(AnalyzeStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    public ValidationErrors validate(AnalyzeStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("catalogName", statement.getCatalogName());
        validationErrors.checkRequiredField("schemaName", statement.getSchemaName());
        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if analyzeColumns columns if null, dont add to sql statement - just use defaults (all columns)
        // If parititon is null, skip
        return validationErrors;
    }

    public Sql[] generateSql(AnalyzeStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("ANALYZE TABLE ");

        if (statement.getCatalogName().trim().length() >= 1) {
            sql.append(statement.getCatalogName() + ".");
        }

        if (statement.getSchemaName().trim().length() >= 1) {
            sql.append(statement.getSchemaName() + ".");
        }

        if (statement.getTableName().trim().length() >= 1) {
            sql.append(statement.getTableName() + " ");
        }

        if (statement.getPartition().size() >= 1) {
            //only supports one partition at a time for now
             Map.Entry<String, String> partitionMap = statement.getPartition().entrySet().iterator().next();

             String partitionColumnName = partitionMap.getKey();
             String partitionColumnValue = partitionMap.getValue();

            // append partition column for each map element, but there should only be one
            sql.append(" PARTITION (" + partitionColumnName + " = '" + partitionColumnValue + "') ");


        }

        if (statement.getAnalyzeColumns().size() >= 1) {
            sql.append("COMPUTE STATISTICS FOR COLUMNS  (" + String.join(", ", statement.getAnalyzeColumns()) + ");");
        }
        else {
            sql.append(" COMPUTE STATISTICS;");
        }
        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}


