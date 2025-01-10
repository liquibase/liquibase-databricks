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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CreateTableGeneratorDatabricks extends CreateTableGenerator {

    private static final String[] PROPERTY_ORDER = {
            "'delta.feature.allowColumnDefaults'",
            "'delta.columnMapping.mode'",
            "'delta.enableDeletionVectors'"
    };

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
        if (!(createStatement.getPartitionColumns().isEmpty()) && !(createStatement.getClusterColumns().isEmpty())) {
            validationErrors.addError("WARNING! Databricks does not supported creating tables with PARTITION and CLUSTER columns, please one supply one option.");
        }
        return validationErrors;
    }

    private String mergeTableProperties(String customProperties) {
        Map<String, String> properties = new LinkedHashMap<>(DEFAULT_VALUES);

        // If there are custom properties, parse and add them
        if (StringUtils.isNotEmpty(customProperties)) {
            Arrays.stream(customProperties.split(","))
                    .map(String::trim)
                    .filter(prop -> !prop.isEmpty())
                    .forEach(prop -> {
                        String[] parts = prop.split("=", 2);
                        if (parts.length == 2) {
                            properties.put(parts[0].trim(), parts[1].trim());
                        }
                    });
        }

        // Build result in specified order
        StringBuilder result = new StringBuilder();

        for (String key : PROPERTY_ORDER) {
            if (properties.containsKey(key)) {
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append(key).append(" = ").append(properties.get(key));
                properties.remove(key);
            }
        }

        // Then add any remaining custom properties
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getValue() != null && !entry.getValue().equals("null")) {
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append(entry.getKey()).append(" = ").append(entry.getValue());
            }
        }

        return result.toString();
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