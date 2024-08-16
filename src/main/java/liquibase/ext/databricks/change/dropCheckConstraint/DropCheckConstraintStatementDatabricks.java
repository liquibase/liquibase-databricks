package liquibase.ext.databricks.change.dropCheckConstraint;

import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DropCheckConstraintStatementDatabricks extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;

}
