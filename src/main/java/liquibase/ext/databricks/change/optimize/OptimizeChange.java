package liquibase.ext.databricks.change.optimize;


import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;

import java.text.MessageFormat;

@DatabaseChange(name = "optimize", description = "Optimize and ZOrder Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class OptimizeChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String zorderColumns;

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

    public String getZorderColumns () {
        return zorderColumns;
    }

    public void setZorderColumns (String zorderColumns) {
        this.zorderColumns = zorderColumns;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully optimized.", getCatalogName(), getSchemaName(), getTableName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        OptimizeStatement statement = new OptimizeStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setZorderColumns(getZorderColumns());

        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;
    }
}
