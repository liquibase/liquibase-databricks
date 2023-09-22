package liquibase.ext.databricks.change.analyzeTable;


import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.statement.SqlStatement;
import java.util.Collections;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;

@DatabaseChange(name = "analyzeTable", description = "Analyze Table Stats", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class AnalyzeTableChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Map<String, String> partition = Collections.emptyMap();

    private ArrayList<String> analyzeColumns = new ArrayList<>();

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

    public Map<String, String> getPartition () {return this.partition;}

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
    protected Change[] createInverses() {
        // No Op for Databricks ANALYZE statement. there is no rolling this back. Its just a stats collection operation
        return new Change[]{
        };
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        AnalyzeTableStatement statement = new AnalyzeTableStatement();

        statement.setCatalogName(getCatalogName());
        statement.setSchemaName(getSchemaName());
        statement.setTableName(getTableName());

        if (getPartition() == null) {
            statement.setPartition(Collections.emptyMap());
        } else {
            statement.setPartition(getPartition());
        }

        if (getAnalyzeColumns() == null) {
            ArrayList<String> noColsArray = new ArrayList<> ();

            statement.setAnalyzeColumns(noColsArray);
        } else {
            statement.setAnalyzeColumns(getAnalyzeColumns());
        }
        SqlStatement[] builtStatement = new SqlStatement[] {statement};

        return builtStatement;
    }
}
