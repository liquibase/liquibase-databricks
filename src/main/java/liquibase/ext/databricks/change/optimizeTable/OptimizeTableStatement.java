package liquibase.ext.databricks.change.optimizeTable;


import liquibase.statement.AbstractSqlStatement;

import java.util.ArrayList;
import java.util.Arrays;



public class OptimizeTableStatement extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private ArrayList<String> zorderColumns = new ArrayList<>();

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName (String catalogName) {
        this.catalogName = catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName (String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName (String schemaName) {
        this.schemaName = schemaName;
    }

    public ArrayList<String> getZorderColumns () {
        return zorderColumns;
    }

    public void setZorderColumns (ArrayList<String> zorderColumns) {
        this.zorderColumns = zorderColumns;
    }

    public void setZorderColumns (String zorderColumns) {
        if (zorderColumns == null) {
            this.zorderColumns = new ArrayList<>();
            return;
        }
        this.zorderColumns = new ArrayList<>(Arrays.asList(zorderColumns.split("\\s*,\\s*")));
    }

}
