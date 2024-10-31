package liquibase.ext.databricks.change.alterCluster;

import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
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
}
