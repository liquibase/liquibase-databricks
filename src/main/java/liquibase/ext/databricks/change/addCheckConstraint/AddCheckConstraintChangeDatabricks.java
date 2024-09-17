package liquibase.ext.databricks.change.addCheckConstraint;

import liquibase.change.*;
import liquibase.database.Database;
import liquibase.ext.databricks.change.dropCheckConstraint.DropCheckConstraintChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;

@DatabaseChange(name = "addCheckConstraint",
        description = "Adds check constraint to Delta Table",
        priority = PrioritizedService.PRIORITY_DATABASE,
        appliesTo = {"column"}
)
@Setter
@Getter
public class AddCheckConstraintChangeDatabricks extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;
    private String constraintBody;

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    // Name of Delta Table Constraint
    @DatabaseChangeProperty(description = "Name of the check constraint")
    public String getConstraintName() {
        return this.constraintName;
    }

    // This is the SQL expression involving the constraint
    @DatabaseChangeProperty(
            serializationType = SerializationType.DIRECT_VALUE
    )
    public String getConstraintBody() {
        return this.constraintBody;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully Added check constraint {3}.", getCatalogName(), getSchemaName(), getTableName(),
                getConstraintName());
    }

    protected Change[] createInverses() {
        DropCheckConstraintChangeDatabricks inverse = new DropCheckConstraintChangeDatabricks();
        inverse.setTableName(getTableName());
        inverse.setSchemaName(getSchemaName());
        inverse.setCatalogName(getCatalogName());
        inverse.setConstraintName(getConstraintName());

        return new Change[]{inverse};
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        AddCheckConstraintStatementDatabricks statement = new AddCheckConstraintStatementDatabricks();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setConstraintName(getConstraintName());
        statement.setConstraintBody(getConstraintBody());

        return new SqlStatement[]{statement};
    }
}
