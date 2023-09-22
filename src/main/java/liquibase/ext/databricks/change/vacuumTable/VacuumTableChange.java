package liquibase.ext.databricks.change.vacuumTable;


import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;

import java.text.MessageFormat;

@DatabaseChange(name = "vacuumTable", description = "Vacuum Old Files from Table", priority = ChangeMetaData.PRIORITY_DEFAULT + 200)
public class VacuumTableChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Integer retentionHours;

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

    public Integer getRetentionHours () {
        return this.retentionHours;
    }

    public void setRetentionHours (Integer retentionHours) {
        this.retentionHours = retentionHours;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully vacuumed.", getCatalogName(), getSchemaName(), getTableName());
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

        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;
    }
}
