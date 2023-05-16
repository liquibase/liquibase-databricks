package liquibase.ext.databricks.change.analyze;


import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;


import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.SqlStatement;
import liquibase.ext.databricks.change.vacuum.VacuumStatement;

@DatabaseChange(name = "analyze", description = "Analyze Table Stats", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class AnalyzeChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Map<String, String> partition;

    private ArrayList<String> analyzeColumns;

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

    public Map<String, String> getPartition () {
        return this.partition;
    }

    public ArrayList<String> getAnalyzeColumns () {return this.analyzeColumns;}

    public void setPartition (Map<String, String> partition) {this.partition = partition;}

    public void setAnalyzeColumns (ArrayList<String> analyzeColumns) {this.analyzeColumns = analyzeColumns;}


    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully analyzed.", getCatalogName(), getSchemaName(), getTableName());
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        AnalyzeStatement statement = new AnalyzeStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());
        statement.setPartition(getPartition());
        statement.setAnalyzeColumns(getAnalyzeColumns());

        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;
    }
}
