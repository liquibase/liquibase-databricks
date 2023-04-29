package liquibase.ext.databricks.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.statement.core.CreateTableStatement;

@DatabaseChange(name = "createTable", description = "Create Table", priority = ChangeMetaData.PRIORITY_DATABASE + 50)
public class PrefixedCreateTableChange extends CreateTableChange {

    private String prefix;

    @DatabaseChangeProperty
    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    protected CreateTableStatement generateCreateTableStatement() {
        String prefix = getPrefix();

        if (prefix == null) {
            setPrefix("");
        } else if (prefix.trim().length() > 0) {
            this.setPrefix(prefix + "_");
        }

        return new CreateTableStatement(getCatalogName(), getSchemaName(), this.getPrefix() + getTableName(), getRemarks());
    }
}