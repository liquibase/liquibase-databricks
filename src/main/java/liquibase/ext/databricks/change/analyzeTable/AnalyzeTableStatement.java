package liquibase.ext.databricks.change.analyzeTable;

import liquibase.statement.AbstractSqlStatement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collections;

public class AnalyzeTableStatement extends AbstractSqlStatement {

    private String catalogName;

    private String schemaName;

    private String tableName;

    private ArrayList<String> analyzeColumns = new ArrayList<>();

    private Map<String, String> partition = Collections.emptyMap();

    public String getCatalogName() {return catalogName;}

    public String getSchemaName() {return schemaName;}

    public String getTableName() {return tableName;}

    public ArrayList<String> getAnalyzeColumns() {return analyzeColumns;}

    public Map<String, String> getPartition() {return partition;}
    public void setCatalogName(String catalogName) { this.catalogName = catalogName;}

    public void setSchemaName(String schemaName) { this.schemaName = schemaName;}

    public void setTableName(String tableName) {this.tableName = tableName;}

    public void setAnalyzeColumns(ArrayList<String> analyzeColumns) {this.analyzeColumns = analyzeColumns;}

    public void setPartition(Map<String, String> partition) {this.partition = partition;}

}
