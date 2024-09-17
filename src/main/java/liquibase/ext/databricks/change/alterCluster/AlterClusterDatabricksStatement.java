package liquibase.ext.databricks.change.alterCluster;

import liquibase.statement.AbstractSqlStatement;

import java.util.List;

public class AlterClusterDatabricksStatement extends AbstractSqlStatement {

    private String tableName;
    private String catalogName;
    private String schemaName;
    private List<ColumnConfig> columns;
    private List<NoneConfig> clusterBy;

    public AlterClusterDatabricksStatement(String tableName, String catalogName, String schemaName) {
        this.tableName = tableName;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnConfig> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnConfig> columns) {
        this.columns = columns;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<NoneConfig> getClusterBy() {
        return clusterBy;
    }

    public void setClusterBy(List<NoneConfig> clusterBy) {
        this.clusterBy = clusterBy;
    }
}
