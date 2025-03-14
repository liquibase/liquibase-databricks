package liquibase.ext.databricks.change.dropCheckConstraint;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;

@Setter
@Getter
@DatabaseChange(name = "dropCheckConstraint", description = "Drops check constraint to Delta Table", priority = PrioritizedService.PRIORITY_DATABASE)
public class DropCheckConstraintChangeDatabricks extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    // Name of Delta Table Constraint
    @DatabaseChangeProperty(
            description = "Name of the check constraint"
    )
    public String getConstraintName() {
        return this.constraintName;
    }


    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully Dropped check constraint {3}.", getCatalogName(), getSchemaName(), getTableName(), getConstraintName());
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

        DropCheckConstraintStatementDatabricks statement = new DropCheckConstraintStatementDatabricks();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setConstraintName(getConstraintName());

        return new SqlStatement[] {statement};
    }
}
