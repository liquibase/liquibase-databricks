package liquibase.ext.databricks.change.addCheckConstraint;

import liquibase.change.*;
import liquibase.database.Database;
import liquibase.ext.databricks.change.dropCheckConstraint.DropCheckConstraintChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.SqlStatement;
import java.text.MessageFormat;
import liquibase.ext.databricks.database.DatabricksDatabase;

@DatabaseChange(name = "addCheckConstraint",
        description = "Adds check constraint to Delta Table",
        priority = DatabricksDatabase.PRIORITY_DEFAULT + 200,
        appliesTo = {"column"}
)
public class AddCheckConstraintChangeDatabricks extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;

    private String constraintName;

    private String constraintBody;

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName (String catalogName) {
        this.catalogName = catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName (String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName (String schemaName) {
        this.schemaName = schemaName;
    }


    // Name of Delta Table Constraint
    @DatabaseChangeProperty(
            description = "Name of the check constraint"
    )
    public String getConstraintName() {
        return this.constraintName;
    }

    public void setConstraintName(String name) {
        this.constraintName = name;
    }


    // The is the SQL expression involving the contraint

    @DatabaseChangeProperty(
            serializationType = SerializationType.DIRECT_VALUE
    )
    public String getConstraintBody() {
        return this.constraintBody;
    }

    public void setConstraintBody(String body) {
        this.constraintBody = body;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully Added check constraint {3}.", getCatalogName(), getSchemaName(), getTableName(), getConstraintName());
    }

    protected Change[] createInverses() {
        DropCheckConstraintChangeDatabricks var1 = new DropCheckConstraintChangeDatabricks();
        var1.setTableName(getTableName());
        var1.setSchemaName(getSchemaName());
        var1.setCatalogName(getCatalogName());
        var1.setConstraintName(getConstraintName());

        return new Change[]{var1};
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        AddCheckConstraintStatementDatabricks statement = new AddCheckConstraintStatementDatabricks();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setConstraintName(getConstraintName());
        statement.setConstraintBody(getConstraintBody());

        return new SqlStatement[] {statement};
    }
}
