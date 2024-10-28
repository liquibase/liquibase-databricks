package liquibase.ext.databricks.change.createTable;

import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.core.CreateTableStatement;
import lombok.Setter;


@DatabaseChange(name = "createTable", description = "Create Table", priority =  PrioritizedService.PRIORITY_DATABASE)
@Setter
public class CreateTableChangeDatabricks extends CreateTableChange {
    private static final String DOUBLE_INIT_ERROR = "Double initialization of extended table properties is not allowed. " +
            "Please avoid using both EXT createTable attributes and Databricks specific extendedTableProperties element. " +
            "Element databricks:extendedTableProperties is preferred way to set databricks specific configurations.";
    private static final String PARTITION_CLUSTER_COLLISION_ERROR = "Databricks does not support CLUSTER columns " +
            "AND PARTITION BY columns, please pick one. And do not supply the other";
    private String tableFormat;
    private String tableLocation;
    private String clusterColumns;
    private String partitionColumns;
    private ExtendedTableProperties extendedTableProperties;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (partitionColumns != null && clusterColumns != null) {
            validationErrors.addError(PARTITION_CLUSTER_COLLISION_ERROR);
        }
        if(extendedTableProperties != null) {
            boolean anyPropertyDuplicated = tableFormat != null && extendedTableProperties.getTableFormat() != null
                    || tableLocation != null && extendedTableProperties.getTableLocation() != null
                    || clusterColumns != null && extendedTableProperties.getClusterColumns() != null
                    || partitionColumns != null && extendedTableProperties.getPartitionColumns() !=null;
            if(anyPropertyDuplicated) {
                validationErrors.addError(DOUBLE_INIT_ERROR);
            }
        }
        return validationErrors;
    }

    @DatabaseChangeProperty
    public String getTableFormat() {return tableFormat;}

    @DatabaseChangeProperty
    public String getTableLocation() {
        return tableLocation;
    }

    @DatabaseChangeProperty
    public String getClusterColumns() {
        return clusterColumns;
    }

    @DatabaseChangeProperty
    public String getPartitionColumns() {return partitionColumns; }

    @Override
    protected CreateTableStatement generateCreateTableStatement() {

        CreateTableStatementDatabricks ctas = new CreateTableStatementDatabricks(getCatalogName(), getSchemaName(), getTableName());

        if(this.getExtendedTableProperties() != null) {
            ctas.setTableLocation(getExtendedTableProperties().getTableLocation());
            ctas.setTableFormat(getExtendedTableProperties().getTableFormat());
            ctas.setClusterColumns(getExtendedTableProperties().getClusterColumns());
            ctas.setPartitionColumns(getExtendedTableProperties().getPartitionColumns());
            ctas.setExtendedTableProperties(this.getExtendedTableProperties());
        } else {
            ctas.setTableFormat(this.getTableFormat());
            ctas.setTableLocation(this.getTableLocation());
            ctas.setClusterColumns(this.getClusterColumns());
            ctas.setPartitionColumns(this.getPartitionColumns());
        }

        return ctas;
    }

    @DatabaseChangeProperty
    public ExtendedTableProperties getExtendedTableProperties() {
        return extendedTableProperties;
    }
}
