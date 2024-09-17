package liquibase.ext.databricks.change.addCheckConstraint;

import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AddCheckConstraintStatementDatabricks extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;
    private String constraintBody;
}
