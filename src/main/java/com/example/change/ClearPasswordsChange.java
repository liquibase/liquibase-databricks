package com.example.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.UpdateDataChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.UpdateStatement;

@DatabaseChange(name = "clearPasswords", description = "Clears all data in a 'password' column", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class ClearPasswordsChange extends AbstractChange {

    private String tableName;
    private String schemaName;
    private String catalogName;

    @DatabaseChangeProperty
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @DatabaseChangeProperty
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @DatabaseChangeProperty
    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public String getConfirmationMessage() {
        return "Passwords cleared";
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[] {
                new UpdateStatement(getCatalogName(), getSchemaName(), getTableName()).addNewColumnValue("password", null)
        };
    }
}
