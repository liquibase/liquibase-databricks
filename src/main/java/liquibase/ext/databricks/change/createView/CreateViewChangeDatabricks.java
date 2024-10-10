package liquibase.ext.databricks.change.createView;

import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateViewChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.ext.databricks.parser.NamespaceDetailsDatabricks;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.core.CreateViewStatement;
import lombok.Setter;


@DatabaseChange(name = "createView", description = "Create View", priority =  PrioritizedService.PRIORITY_DATABASE)
@Setter
public class CreateViewChangeDatabricks extends CreateViewChange {
    private String tblProperties;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    protected CreateViewStatement createViewStatement(String catalogName, String schemaName, String viewName, String selectQuery, boolean replaceIfExists) {
        CreateViewStatementDatabricks cvsd = new CreateViewStatementDatabricks(catalogName, schemaName, viewName, selectQuery, replaceIfExists);
        cvsd.setTblProperties(this.getTblProperties());
        return cvsd;
    }

    @DatabaseChangeProperty
    public String getTblProperties() {
        return tblProperties;
    }

    @Override
    public String getSerializedObjectNamespace() {
        return NamespaceDetailsDatabricks.DATABRICKS_NAMESPACE;
    }

}
