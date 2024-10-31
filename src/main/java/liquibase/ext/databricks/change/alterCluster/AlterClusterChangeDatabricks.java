package liquibase.ext.databricks.change.alterCluster;

import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.databricks.parser.NamespaceDetailsDatabricks.DATABRICKS_NAMESPACE;

@Setter
@DatabaseChange(name = "alterCluster", description = "Alter Cluster", priority = PrioritizedService.PRIORITY_DATABASE +500)
public class AlterClusterChangeDatabricks extends AbstractChange {

    private String tableName;
    private String catalogName;
    private String schemaName;
    private List<ColumnConfig> columns;
    private List<NoneConfig> clusterBy;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (columns == null && clusterBy == null) {
            validationErrors.addError("Alter Cluster change require list of columns or element 'ClusterBy', please add at least one option.");
        }
        return validationErrors;
    }

    public AlterClusterChangeDatabricks() {
        super();
        columns = new ArrayList<>();
        clusterBy = new ArrayList<>();
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully altered.", getCatalogName(), getSchemaName(), getTableName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        AlterClusterDatabricksStatement statement = new AlterClusterDatabricksStatement(tableName, catalogName, schemaName);
        if (getColumns() != null && !getColumns().isEmpty()) {
            statement.setColumns(getColumns());
        } else if (getClusterBy() != null && !getClusterBy().isEmpty()) {
            statement.setClusterBy(getClusterBy());
        }
        return new SqlStatement[]{statement};
    }

    @DatabaseChangeProperty
    public String getTableName() {
        return tableName;
    }

    @DatabaseChangeProperty
    public List<ColumnConfig> getColumns() {
        if (columns == null) {
            return new ArrayList<>();
        }
        return columns;
    }

    @DatabaseChangeProperty
    public String getCatalogName() {
        return catalogName;
    }

    @DatabaseChangeProperty
    public String getSchemaName() {
        return schemaName;
    }

    @DatabaseChangeProperty
    public List<NoneConfig> getClusterBy() {
        if (clusterBy == null) {
            return new ArrayList<>();
        }
        return clusterBy;
    }

    @Override
    public String getSerializedObjectNamespace() {
        return DATABRICKS_NAMESPACE;
    }
}
