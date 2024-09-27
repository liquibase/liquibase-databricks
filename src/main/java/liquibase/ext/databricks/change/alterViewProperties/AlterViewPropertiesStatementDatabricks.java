package liquibase.ext.databricks.change.alterViewProperties;

import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AlterViewPropertiesStatementDatabricks extends AbstractSqlStatement {

    private String viewName;
    private String catalogName;
    private String schemaName;
    private SetExtendedTableProperties setExtendedTableProperties;
    private UnsetExtendedTableProperties unsetExtendedTableProperties;

    public AlterViewPropertiesStatementDatabricks(String catalogName, String schemaName, String viewName) {
        this.viewName = viewName;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

}
