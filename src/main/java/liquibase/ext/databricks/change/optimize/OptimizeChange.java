package liquibase.ext.databricks.change.optimize;



import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.statement.SqlStatement;
import liquibase.ext.databricks.change.optimize.OptimizeStatement;
import liquibase.ext.databricks.change.optimize.OptimizeGenerator;

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
