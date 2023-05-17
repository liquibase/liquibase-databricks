package liquibase.ext.databricks.change.createTable;


import liquibase.statement.core.CreateTableStatement;
public class CreateTableStatementDatabricks extends CreateTableStatement{

    private String tableFormat = "delta";
    private String tableLocation = "";


    public CreateTableStatementDatabricks(String catalogName, String schemaName, String tableName) {
        super(catalogName, schemaName, tableName);
    }


    public void setTableFormat(String tableFormat) {this.tableFormat = tableFormat;}

    public String getTableFormat() {return this.tableFormat;}

    public void setTableLocation(String tableLocation) { this.tableLocation = tableLocation;}

    public String getTableLocation() {return this.tableLocation;}


}
