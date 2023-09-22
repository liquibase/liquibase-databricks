package liquibase.ext.databricks.change.createTable;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.statement.core.CreateTableStatement;


@DatabaseChange(name = "createTable", description = "Create Table", priority = ChangeMetaData.PRIORITY_DATABASE +500)
public class CreateTableChangeDatabricks extends CreateTableChange {

    private String tableFormat;
    private String tableLocation;

    private String clusterColumns;

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

    public void setTableLocation(String tableLocation) {this.tableLocation = tableLocation;}

    @DatabaseChangeProperty
    public void setClusterColumns(String clusterColumns) {
        this.clusterColumns =  clusterColumns;
    }


    @Override
    protected CreateTableStatement generateCreateTableStatement() {

        CreateTableStatementDatabricks ctas = new CreateTableStatementDatabricks(getCatalogName(), getSchemaName(), getTableName());

        ctas.setTableFormat(this.getTableFormat());
        ctas.setTableLocation(this.getTableLocation());
        ctas.setClusterColumns(this.getClusterColumns());

        return ctas;
    }
}