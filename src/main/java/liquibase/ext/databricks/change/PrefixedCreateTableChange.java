package liquibase.ext.databricks.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.statement.core.CreateTableStatement;
import liquibase.util.StringUtil;

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
        String prefix = StringUtil.trimToEmpty(getPrefix());

        if (!prefix.equals("")) {
            prefix = prefix + "_";
        }

        return new CreateTableStatement(getCatalogName(), getSchemaName(), prefix + getTableName());
    }
}