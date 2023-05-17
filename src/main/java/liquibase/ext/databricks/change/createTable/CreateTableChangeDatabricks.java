package liquibase.ext.databricks.change.createTable;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;



@DatabaseChange(name = "createTable", description = "Create Table", priority = ChangeMetaData.PRIORITY_DATABASE +500)
public class CreateTableChangeDatabricks extends CreateTableChange {

    private String tableFormat;
    private String tableLocation;

    @DatabaseChangeProperty
    public String getTableFormat() {return tableFormat;}

    public void setTableFormat(String tableFormat) {this.tableFormat = tableFormat;}

    @DatabaseChangeProperty
    public String getTableLocation() {
        return tableLocation;
    }

    public void setTableLocation(String tableLocation) {this.tableLocation = tableLocation;}



    @Override
    protected CreateTableStatementDatabricks generateCreateTableStatement() {

        CreateTableStatementDatabricks ctas = new CreateTableStatementDatabricks(getCatalogName(), getSchemaName(), getTableName());

        ctas.setTableFormat(this.getTableFormat());
        ctas.setTableLocation((this.getTableLocation()));

        return ctas;
    }
}