package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.change.alterCluster.AlterClusterDatabricksStatement;
import liquibase.ext.databricks.change.alterCluster.ColumnConfig;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class AlterClusterGeneratorDatabricks extends AbstractSqlGenerator<AlterClusterDatabricksStatement> {
    @Override
    public ValidationErrors validate(AlterClusterDatabricksStatement statement, Database database, SqlGeneratorChain<AlterClusterDatabricksStatement> sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        if (statement.getClusterBy() == null && statement.getColumns() == null){
            validationErrors.addError("WARNING! Alter Cluster change require list of columns or element 'ClusterBy', please add at least one option.");
        }
        if (statement.getClusterBy() != null && (statement.getClusterBy().isEmpty() || !statement.getClusterBy().get(0).getNone().equals("true"))) {
            validationErrors.addError("WARNING! ClusterBy attribute require attribute 'none=\"true\"'");
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(AlterClusterDatabricksStatement statement, Database database, SqlGeneratorChain<AlterClusterDatabricksStatement> sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("ALTER TABLE ");
        buffer.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));
        buffer.append(" CLUSTER BY ");
        if (statement.getColumns() != null && !statement.getColumns().isEmpty()) {
            buffer.append("(");
            for (ColumnConfig column : statement.getColumns()) {
                buffer.append(column.getName());
                buffer.append(",");
            }
            buffer.deleteCharAt(buffer.length() - 1);
            buffer.append(")");
        } else if (statement.getClusterBy() != null && !statement.getClusterBy().isEmpty()) {
            buffer.append("NONE");
        }

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }

}
