package com.example.change;

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
            prefix = "standard";
        }

        return new CreateTableStatement(getCatalogName(), getSchemaName(), prefix + "_" + getTableName(), getRemarks());
    }
}
