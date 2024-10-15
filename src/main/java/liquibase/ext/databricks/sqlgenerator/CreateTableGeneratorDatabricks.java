package liquibase.ext.databricks.sqlgenerator;


import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.createTable.CreateTableStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.DatabaseObject;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class CreateTableGeneratorDatabricks extends CreateTableGenerator {

    private static final String CLUSTERING_INFORMATION_TBL_PROPERTY_START = "clusteringColumns=[[";


    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return super.supports(statement, database) && (database instanceof DatabricksDatabase);
    }

    public ValidationErrors validate(CreateTableStatementDatabricks createStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        if (!(createStatement.getPartitionColumns().isEmpty()) && !(createStatement.getClusterColumns().isEmpty())){
            validationErrors.addError("WARNING! Databricks does not supported creating tables with PARTITION and CLUSTER columns, please one supply one option.");
        }
        return validationErrors;
    }


    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
        StringBuilder finalsql = new StringBuilder(sqls[0].toSql());

        if (statement instanceof CreateTableStatementDatabricks) {
            CreateTableStatementDatabricks thisStatement = (CreateTableStatementDatabricks) statement;

            if ((!StringUtils.isEmpty(thisStatement.getTableFormat()))) {
                finalsql.append(" USING ").append(thisStatement.getTableFormat());
            } else if (thisStatement.getExtendedTableProperties() != null && StringUtils.isNoneEmpty(thisStatement.getExtendedTableProperties().getTblProperties())) {
                finalsql.append(" TBLPROPERTIES (").append(avoidClusterProperties(thisStatement)).append(")");
            } else {
                finalsql.append(" USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = true)");
            }

            // Databricks can decide to have tables live in a particular location. If null, Databricks will handle the location automatically in DBFS
            if (!StringUtils.isEmpty(thisStatement.getTableLocation())) {
                finalsql.append(" LOCATION '").append(thisStatement.getTableLocation()).append("'");
            } else if (thisStatement.getExtendedTableProperties() != null && StringUtils.isNotEmpty(thisStatement.getExtendedTableProperties().getTableLocation())) {
                finalsql.append(" LOCATION '").append(thisStatement.getExtendedTableProperties().getTableLocation()).append("'");
            }

            ArrayList<String> clusterCols = thisStatement.getClusterColumns();
            ArrayList<String> partitionCols = thisStatement.getPartitionColumns();


            // If there are any cluster columns, add the clause
            // ONLY if there are NOT cluster columns, then do partitions, but never both.
            if (!clusterCols.isEmpty()) {

                finalsql.append(" CLUSTER BY (");

                int val = 0;
                while (clusterCols.size() > val) {
                    finalsql.append(clusterCols.get(val));

                    val +=1;
                    if (clusterCols.size() > val) {
                        finalsql.append(", ");
                    }
                    else {
                        finalsql.append(")");
                    }
                }
            } else if (!partitionCols.isEmpty()) {
                finalsql.append(" PARTITIONED BY (");

                int val = 0;
                while (partitionCols.size() > val) {
                    finalsql.append(partitionCols.get(val));

                    val +=1;
                    if (partitionCols.size() > val) {
                        finalsql.append(", ");
                    }
                    else {
                        finalsql.append(")");
                    }
                }
            }


        }

        sqls[0] = new UnparsedSql(finalsql.toString(), sqls[0].getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));

        return sqls;

    }

    /**
     * While we are passing TBLPROPERTIES as raw string into create table statement, especially in cases of
     * changelog generation we need to sanitize them from 'clusteringColumns' property, otherwise generated changelog
     * will fail to execute.
     * Parsing of tblProperties map as an actual Map structured collection should make this approach safer and easier.
     * @param statement CreateTableStatementDatabricks containing tblProperties raw string
     * @return tblProperties string without 'clusteringColumns' property if it was present, otherwise untouched
     * tblProperties raw string.
     * */
    private String avoidClusterProperties(CreateTableStatementDatabricks statement) {
        String tblProperties = statement.getExtendedTableProperties().getTblProperties();
        if(tblProperties.contains(CLUSTERING_INFORMATION_TBL_PROPERTY_START)) {
            int clusterColumnsStartIndex = tblProperties.indexOf(CLUSTERING_INFORMATION_TBL_PROPERTY_START);
            String replaceString = tblProperties.substring(clusterColumnsStartIndex, tblProperties.indexOf("\"]],", clusterColumnsStartIndex) + 4);
            return tblProperties.replace(replaceString, "");
        }
        return tblProperties;
    }

}
