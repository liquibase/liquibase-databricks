package liquibase.ext.databricks.sqlgenerator;


import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.createTable.CreateTableStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.DatabaseObject;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static java.util.stream.Collectors.joining;

public class CreateTableGeneratorDatabricks extends CreateTableGenerator {

    private static final Map<String, String> DEFAULT_VALUES = Map.of(
            "'delta.feature.allowColumnDefaults'", "'supported'",
            "'delta.columnMapping.mode'", "'name'",
            "'delta.enableDeletionVectors'", "true"
    );

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

    private String mergeTableProperties(String customProperties) {

        // First, ensure all essential properties are present with default values
        Map<String, String> properties = new LinkedHashMap<>(DEFAULT_VALUES);

        // If there are custom properties, parse and add them
        if (StringUtils.isNotEmpty(customProperties)) {
            Arrays.stream(customProperties.split(","))
                  .map(String::trim)
                  .filter(prop -> !prop.isEmpty())
                  .forEach(prop -> {
                      String[] parts = prop.split("=", 2);
                      if (parts.length == 2) {
                          String key = parts[0].trim();
                          String value = parts[1].trim();
                          // When updating default properties, maintain their position
                          if (DEFAULT_VALUES.containsKey(key)) {
                              properties.put(key, value);
                          } else {
                              // For non-default properties, add to the end
                              properties.remove(key);
                              properties.put(key, value);
                          }
                      }
                  });
        }

        return properties.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().equals("null"))
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(joining(", ")).trim();
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
        StringBuilder finalsql = new StringBuilder(sqls[0].toSql());

        if (statement instanceof CreateTableStatementDatabricks) {
            CreateTableStatementDatabricks thisStatement = (CreateTableStatementDatabricks) statement;

            if ((!StringUtils.isEmpty(thisStatement.getTableFormat()))) {
                finalsql.append(" USING ").append(thisStatement.getTableFormat());
            } else {
                finalsql.append(" USING delta");
            }

            String properties = null;
            if (thisStatement.getExtendedTableProperties() != null) {
                properties = thisStatement.getExtendedTableProperties().getTblProperties();
            }
            finalsql.append(" TBLPROPERTIES(").append(mergeTableProperties(properties)).append(")");

            if (!StringUtils.isEmpty(thisStatement.getTableLocation())) {
                finalsql.append(" LOCATION '").append(thisStatement.getTableLocation()).append("'");
            } else if (thisStatement.getExtendedTableProperties() != null && StringUtils.isNotEmpty(thisStatement.getExtendedTableProperties().getTableLocation())) {
                finalsql.append(" LOCATION '").append(thisStatement.getExtendedTableProperties().getTableLocation()).append("'");
            }

            List<String> clusterCols = thisStatement.getClusterColumns();
            List<String> partitionCols = thisStatement.getPartitionColumns();

            if (!clusterCols.isEmpty()) {
                finalsql.append(" CLUSTER BY (");
                int val = 0;
                while (clusterCols.size() > val) {
                    finalsql.append(clusterCols.get(val));
                    val += 1;
                    if (clusterCols.size() > val) {
                        finalsql.append(", ");
                    } else {
                        finalsql.append(")");
                    }
                }
            } else if (!partitionCols.isEmpty()) {
                finalsql.append(" PARTITIONED BY (");
                int val = 0;
                while (partitionCols.size() > val) {
                    finalsql.append(partitionCols.get(val));
                    val += 1;
                    if (partitionCols.size() > val) {
                        finalsql.append(", ");
                    } else {
                        finalsql.append(")");
                    }
                }
            }
        }

        sqls[0] = new UnparsedSql(finalsql.toString(), sqls[0].getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));
        return sqls;
    }
}