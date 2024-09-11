package liquibase.ext.databricks.change.createView;

import liquibase.statement.core.CreateViewStatement;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateViewStatementDatabricks extends CreateViewStatement {

    private String tblProperties;

    public CreateViewStatementDatabricks(String catalogName, String schemaName, String viewName, String selectQuery, boolean replaceIfExists) {
        super(catalogName, schemaName, viewName, selectQuery, replaceIfExists);
    }
}
