package liquibase.ext.databricks.change.alterTableProperties;

import liquibase.ext.databricks.change.AbstractAlterPropertiesStatementDatabricks;
import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AlterTablePropertiesStatementDatabricks extends AbstractAlterPropertiesStatementDatabricks {

    private String tableName;

    public AlterTablePropertiesStatementDatabricks(String catalogName, String schemaName, String tableName) {
        super(catalogName, schemaName);
        this.tableName = tableName;
    }

}
