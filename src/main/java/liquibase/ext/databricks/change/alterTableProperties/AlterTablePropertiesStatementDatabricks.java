package liquibase.ext.databricks.change.alterTableProperties;

import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AlterTablePropertiesStatementDatabricks extends AbstractSqlStatement {

    private String tableName;
    private String catalogName;
    private String schemaName;
    private SetExtendedTableProperties setExtendedTableProperties;
    private UnsetExtendedTableProperties unsetExtendedTableProperties;

    public AlterTablePropertiesStatementDatabricks(String catalogName, String schemaName, String tableName) {
        this.tableName = tableName;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

}
