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
import liquibase.util.StringUtil;

import java.util.ArrayList;

public class CreateTableGeneratorDatabricks extends CreateTableGenerator {


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
        String finalsql = sqls[0].toSql();

        if (statement instanceof CreateTableStatementDatabricks) {
            CreateTableStatementDatabricks thisStatement = (CreateTableStatementDatabricks) statement;

            if ((!StringUtil.isEmpty(thisStatement.getTableFormat()))) {
                finalsql += " USING " + thisStatement.getTableFormat();
            } else {
                finalsql += " USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')";
            }

            // Databricks can decide to have tables live in a particular location. If null, Databricks will handle the location automatically in DBFS
            if (!StringUtil.isEmpty(thisStatement.getTableLocation())) {
                finalsql += " LOCATION '" + thisStatement.getTableLocation() + "'";
            }

            ArrayList<String> clusterCols = thisStatement.getClusterColumns();
            ArrayList<String> partitionCols = thisStatement.getPartitionColumns();


            // If there are any cluster columns, add the clause
            // ONLY if there are NOT cluster columns, then do partitions, but never both.
            if (clusterCols.size() >= 1 ) {

                finalsql += " CLUSTER BY (";

                int val = 0;
                while (clusterCols.size() > val) {
                    finalsql += clusterCols.get(val);

                    val +=1;
                    if (clusterCols.size() > val) {
                        finalsql += ", ";
                    }
                    else {
                        finalsql += ")";
                    }
                }
            } else if (partitionCols.size() >=1) {
                finalsql += " PARTITIONED BY (";

                int val = 0;
                while (partitionCols.size() > val) {
                    finalsql += partitionCols.get(val);

                    val +=1;
                    if (partitionCols.size() > val) {
                        finalsql += ", ";
                    }
                    else {
                        finalsql += ")";
                    }
                }
            }


        } else {
            // Not a Delta Table
            finalsql += "";
        }

        //}

        sqls[0] = new UnparsedSql(finalsql, sqls[0].getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));

        return sqls;

    }

}
