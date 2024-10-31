package liquibase.ext.databricks.change.vacuumTable;


import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class VacuumTableStatement extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Integer retentionHours;

}
