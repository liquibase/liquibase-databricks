package liquibase.ext.databricks.change.createTable;

import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.core.CreateTableStatement;


@DatabaseChange(name = "createTable", description = "Create Table", priority = PrioritizedService.PRIORITY_DATABASE +500)
public class CreateTableChangeDatabricks extends CreateTableChange {

    private String tableFormat;
    private String tableLocation;
    private String clusterColumns;
    private String partitionColumns;
    private ExtendedTableProperties extendedTableProperties;


    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (partitionColumns != null && clusterColumns != null) {
                    validationErrors.addError("Databricks does not support CLUSTER columns AND PARTITION BY columns, please pick one. And do not supply the other");
        }
        return validationErrors;
    }

    @DatabaseChangeProperty
    public String getTableFormat() {return tableFormat;}

    public void setTableFormat(String tableFormat) {this.tableFormat = tableFormat;}

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

    public void setTableLocation(String tableLocation) {this.tableLocation = tableLocation;}

    @DatabaseChangeProperty
    public void setClusterColumns(String clusterColumns) {
        this.clusterColumns =  clusterColumns;
    }

    @DatabaseChangeProperty
    public void setPartitionColumns(String partitionColumns) { this.partitionColumns = partitionColumns; }

    @Override
    protected CreateTableStatement generateCreateTableStatement() {

        CreateTableStatementDatabricks ctas = new CreateTableStatementDatabricks(getCatalogName(), getSchemaName(), getTableName());

        ctas.setTableFormat(this.getTableFormat());
        ctas.setTableLocation(this.getTableLocation());
        ctas.setClusterColumns(this.getClusterColumns());
        ctas.setPartitionColumns(this.getPartitionColumns());
        ctas.setExtendedTableProperties(this.getExtendedTableProperties());

        return ctas;
    }

    @DatabaseChangeProperty
    public ExtendedTableProperties getExtendedTableProperties() {
        return extendedTableProperties;
    }

    public void setExtendedTableProperties(ExtendedTableProperties extendedTableProperties) {
        this.extendedTableProperties = extendedTableProperties;
    }
}
