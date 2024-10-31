package liquibase.ext.databricks.change.vacuumTable;


import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;

@Setter
@Getter
@DatabaseChange(name = "vacuumTable", description = "Vacuum Old Files from Table", priority =  PrioritizedService.PRIORITY_DATABASE)
public class VacuumTableChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Integer retentionHours;

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully vacuumed.", getCatalogName(), getSchemaName(), getTableName());
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    protected Change[] createInverses() {
        // No Op for Databricks optimize statement. there is no rolling this back.
        // There is no rolling this back, it is a permanent operation
        return new Change[]{
        };
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        VacuumTableStatement statement = new VacuumTableStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        // Check for optional retention hours

        if (getRetentionHours() == null) {
            // Default to table default of 168 hours
            statement.setRetentionHours(168);

        } else {
            statement.setRetentionHours(getRetentionHours());
        }

        return new SqlStatement[] {statement};
    }
}
