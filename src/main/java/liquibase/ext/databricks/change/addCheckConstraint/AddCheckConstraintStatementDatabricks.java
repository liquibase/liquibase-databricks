package liquibase.ext.databricks.change.addCheckConstraint;

import liquibase.statement.AbstractSqlStatement;

public class AddCheckConstraintStatementDatabricks extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String constraintName;
    private String constraintBody;


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

    public String getConstraintBody() {
        return this.constraintBody;
    }

    public void setConstraintBody(String constraintBody) {
        this.constraintBody = constraintBody;
    }
}
