package liquibase.ext.databricks.change.createTable;


import liquibase.statement.core.CreateTableStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class CreateTableStatementDatabricks extends CreateTableStatement {

    private String tableFormat;
    private String tableLocation;
    private List<String> clusterColumns;
    private List<String> partitionColumns;
    private ExtendedTableProperties extendedTableProperties;


    public CreateTableStatementDatabricks(String catalogName, String schemaName, String tableName) {
        super(catalogName, schemaName, tableName);
    }

    public void setPartitionColumns(String partitionColumns) {
        if (partitionColumns == null) {
            this.partitionColumns = new ArrayList<>();
            return;
        }
        this.partitionColumns = new ArrayList<>(Arrays.asList(partitionColumns.split("\\s*,\\s*")));
    }

    public void setClusterColumns(String clusterColumns) {
        if (clusterColumns == null) {
            this.clusterColumns = new ArrayList<>();
            return;
        }
        this.clusterColumns = new ArrayList<>(Arrays.asList(clusterColumns.split("\\s*,\\s*")));
    }
}
