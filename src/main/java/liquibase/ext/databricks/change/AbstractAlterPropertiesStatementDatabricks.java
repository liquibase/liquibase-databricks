package liquibase.ext.databricks.change;

import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class AbstractAlterPropertiesStatementDatabricks extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private SetExtendedTableProperties setExtendedTableProperties;
    private UnsetExtendedTableProperties unsetExtendedTableProperties;

    public AbstractAlterPropertiesStatementDatabricks(String catalogName, String schemaName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

}
