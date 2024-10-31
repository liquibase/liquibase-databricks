package liquibase.ext.databricks.change.analyzeTable;


import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

import java.util.Map;

public class AnalyzeTableGenerator extends AbstractSqlGenerator<AnalyzeTableStatement> {

    @Override
    //check support for optimizer operation
    public boolean supports(AnalyzeTableStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(AnalyzeTableStatement statement, Database database, SqlGeneratorChain chain){

        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("tableName", statement.getTableName());

        // if analyzeColumns columns if null, dont add to sql statement - just use defaults (all columns)
        // If partition is null, skip
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AnalyzeTableStatement statement, Database database, SqlGeneratorChain chain) {

        StringBuilder sql = new StringBuilder("ANALYZE TABLE ");

        sql.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));

        if (!statement.getPartition().isEmpty()) {
            //only supports one partition at a time for now
             Map.Entry<String, String> partitionMap = statement.getPartition().entrySet().iterator().next();

             String partitionColumnName = partitionMap.getKey();
             String partitionColumnValue = partitionMap.getValue();

            // append partition column for each map element, but there should only be one
            sql.append(" PARTITION (").append(partitionColumnName).append(" = '").append(partitionColumnValue).append("') ");


        }

        if (!statement.getAnalyzeColumns().isEmpty()) {
            sql.append("COMPUTE STATISTICS FOR COLUMNS  (").append(String.join(", ", statement.getAnalyzeColumns())).append(")");
        }
        else {
            sql.append(" COMPUTE STATISTICS");
        }
        return new Sql[] { new UnparsedSql(sql.toString()) };

    }
}


