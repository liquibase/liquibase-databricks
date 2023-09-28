package liquibase.ext.databricks.change.createTable;


import liquibase.statement.core.CreateTableStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateTableStatementDatabricks extends CreateTableStatement {

    private String tableFormat;
    private String tableLocation;

    private ArrayList<String> clusterColumns;

    private ArrayList<String> partitionColumns;


    public CreateTableStatementDatabricks(String catalogName, String schemaName, String tableName) {
        super(catalogName, schemaName, tableName);
    }

    public void setTableFormat(String tableFormat) {this.tableFormat = tableFormat;}

    public String getTableFormat() {return this.tableFormat;}

    public void setTableLocation(String tableLocation) { this.tableLocation = tableLocation;}

    public String getTableLocation() {return this.tableLocation;}
    public ArrayList<String> getClusterColumns () {
        return clusterColumns;
    }

    public ArrayList<String> getPartitionColumns () {
        return partitionColumns;
    }


    public void setPartitionColumns (String partitionColumns) {
        if (partitionColumns == null) {
            this.partitionColumns = new ArrayList<>();
            return;
        }
        this.partitionColumns = new ArrayList<>(Arrays.asList(partitionColumns.split("\\s*,\\s*")));
    }



    public void setClusterColumns (String clusterColumns) {
        if (clusterColumns == null) {
            this.clusterColumns = new ArrayList<>();
            return;
        }
        this.clusterColumns = new ArrayList<>(Arrays.asList(clusterColumns.split("\\s*,\\s*")));
    }


}
