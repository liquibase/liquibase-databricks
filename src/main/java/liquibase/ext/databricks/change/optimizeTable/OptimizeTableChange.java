package liquibase.ext.databricks.change.optimizeTable;


import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.change.Change;

import java.text.MessageFormat;
import java.util.ArrayList;

@DatabaseChange(name = "optimizeTable", description = "Optimize and ZOrder Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class OptimizeTableChange extends AbstractChange {

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
    protected Change[] createInverses() {
        // No Op for Databricks optimize statement. there is no rolling this back.
        // you COULD roll this back, but you do not want a system to run RESTORE version as of dynamically. That is risky.
        return new Change[]{
        };
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        OptimizeTableStatement statement = new OptimizeTableStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());

        // Check for ZORDER columns
        if (getZorderColumns() == null) {
            ArrayList<String> noZorderCol = new ArrayList<>();
            statement.setZorderColumns(noZorderCol);
        } else {
            statement.setZorderColumns(getZorderColumns());
        }
        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;

    }
}
