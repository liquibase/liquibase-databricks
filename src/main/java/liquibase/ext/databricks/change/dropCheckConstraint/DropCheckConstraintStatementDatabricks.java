package liquibase.ext.databricks.change.dropCheckConstraint;

import liquibase.statement.AbstractSqlStatement;

public class DropCheckConstraintStatementDatabricks extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;
    private boolean validate = true;


    public String getCatalogName() {
        return this.catalogName;
    }

    public void setCatalogName(String catalogName) {this.catalogName = catalogName;}


    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(String schemaName) {this.schemaName = schemaName;}

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {this.tableName = tableName;}

    public String getConstraintName() {
        return this.constraintName;
    }

    public void setConstraintName(String constraintName) {this.constraintName = constraintName;}

}
