package liquibase.ext.databricks.change.vacuum;


import java.text.MessageFormat;


import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.ext.databricks.change.vacuum.VacuumStatement;

@DatabaseChange(name = "vacuum", description = "Vacuum Old Files from Table", priority = ChangeMetaData.PRIORITY_DEFAULT + 200)
public class VacuumChange extends AbstractChange {

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
    public SqlStatement[] generateStatements(Database database) {

        VacuumStatement statement = new VacuumStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setRetentionHours(getRetentionHours());

        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;
    }
}
