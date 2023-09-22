package liquibase.ext.databricks.change.vacuumTable;


import liquibase.statement.AbstractSqlStatement;

public class VacuumTableStatement extends AbstractSqlStatement {

    private String catalogName;

    private String schemaName;

    private String tableName;

    private Integer retentionHours;


    public String getCatalogName() {return this.catalogName;}

    public String getSchemaName() {return this.schemaName;}

    public String getTableName() {return this.tableName;}

    public Integer getRetentionHours() {return this.retentionHours;}


    public void setCatalogName(String catalogName) { this.catalogName = catalogName;}

    public void setSchemaName(String schemaName) { this.schemaName = schemaName;}

    public void setTableName(String tableName) {this.tableName = tableName;}

    public void setRetentionHours(Integer retentionHours) {this.retentionHours = retentionHours;}

}
