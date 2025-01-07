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
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static java.util.stream.Collectors.joining;

public class CreateTableGeneratorDatabricks extends CreateTableGenerator {

    private static final Set<String> ESSENTIAL_PROPERTIES = Set.of(
            "'delta.columnMapping.mode'",
            "'delta.enableDeletionVectors'",
            "'delta.feature.allowColumnDefaults'",
            "'delta.logRetentionDuration'",
            "'delta.enableChangeDataFeed'"
    );

    private static final Map<String, String> DEFAULT_VALUES = Map.of(
            "'delta.columnMapping.mode'", "'name'",
            "'delta.enableDeletionVectors'", "true",
            "'delta.feature.allowColumnDefaults'", "'supported'"
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


    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
        String baseSQL = sqls[0].toSql();
        baseSQL = baseSQL.substring(0, baseSQL.lastIndexOf(")")) + ")";
        StringBuilder finalsql = new StringBuilder(baseSQL);
        CreateTableStatementDatabricks thisStatement = (CreateTableStatementDatabricks) statement;

        //if (statement instanceof CreateTableStatementDatabricks) {

            if (!StringUtils.isEmpty(thisStatement.getTableFormat())) {
                finalsql.append(" USING ").append(thisStatement.getTableFormat());
            } else {
                finalsql.append(" USING delta");
            }

            if (thisStatement.getExtendedTableProperties() != null) {
                String properties = processTableProperties(
                        thisStatement.getExtendedTableProperties().getTblProperties()
                );
                finalsql.append(" TBLPROPERTIES (").append(properties).append(")");
            } else {
                String defaultProperties = processTableProperties("");
                finalsql.append(" TBLPROPERTIES (").append(defaultProperties).append(")");
            }

            // Databricks can decide to have tables live in a particular location. If null, Databricks will handle the location automatically in DBFS
            if (!StringUtils.isEmpty(thisStatement.getTableLocation())) {
                finalsql.append(" LOCATION '").append(thisStatement.getTableLocation()).append("'");
            }

            List<String> clusterCols = thisStatement.getClusterColumns();
            List<String> partitionCols = thisStatement.getPartitionColumns();


            // If there are any cluster columns, add the clause
            // ONLY if there are NO cluster columns, then do partitions, but never both.
            if (!clusterCols.isEmpty()) {
                finalsql.append(" CLUSTER BY (")
                        .append(String.join(", ", clusterCols))
                        .append(")");
            } else if (!partitionCols.isEmpty()) {
                finalsql.append(" PARTITIONED BY (")
                        .append(String.join(", ", partitionCols))
                        .append(")");
            }
      //  }

        return new Sql[]{new UnparsedSql(finalsql.toString(), getAffectedTable(thisStatement))};
    }

    private String processTableProperties(String userProperties) {
        // If there are no user properties, we return all essential properties with their default values
        if (StringUtils.isEmpty(userProperties)) {
            return ESSENTIAL_PROPERTIES.stream()
                    .map(prop -> prop + " = " + DEFAULT_VALUES.get(prop))
                    .collect(joining(", "));
        }

        // convert user properties into a Map for processing
        Map<String, String> properties = new LinkedHashMap<>();

        if (!StringUtils.isEmpty(userProperties)) {
            Arrays.stream(userProperties.split(","))
                    .map(String::trim)
                    .map(prop -> prop.split("="))
                    .forEach(parts -> {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        properties.put(key, value);
                    });
        }

        // For each essential property, we check whether it exists
        // If it doesn't exist, we add it with the default value
        for (String essentialProp : ESSENTIAL_PROPERTIES) {
            if (!properties.containsKey(essentialProp)) {
                properties.put(essentialProp, DEFAULT_VALUES.get(essentialProp));
            }
        }

        // convert the Map back to string
        return properties.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(joining(", "));
    }
}
