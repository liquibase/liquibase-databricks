package liquibase.ext.databricks.change.alterViewProperties;

import liquibase.ext.databricks.change.AbstractAlterPropertiesStatementDatabricks;
import liquibase.ext.databricks.change.alterTableProperties.SetExtendedTableProperties;
import liquibase.ext.databricks.change.alterTableProperties.UnsetExtendedTableProperties;
import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AlterViewPropertiesStatementDatabricks extends AbstractAlterPropertiesStatementDatabricks {

    private String viewName;

    public AlterViewPropertiesStatementDatabricks(String catalogName, String schemaName, String viewName) {
        super(catalogName, schemaName);
        this.viewName = viewName;
    }

}
