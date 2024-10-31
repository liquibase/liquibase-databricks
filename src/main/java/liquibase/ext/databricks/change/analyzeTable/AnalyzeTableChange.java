package liquibase.ext.databricks.change.analyzeTable;


import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@DatabaseChange(name = "analyzeTable", description = "Analyze Table Stats", priority =  PrioritizedService.PRIORITY_DATABASE)
public class AnalyzeTableChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Map<String, String> partition = Collections.emptyMap();

    private List<String> analyzeColumns = new ArrayList<>();

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
            List<String> noColsArray = new ArrayList<> ();

            statement.setAnalyzeColumns(noColsArray);
        } else {
            statement.setAnalyzeColumns(getAnalyzeColumns());
        }

        return new SqlStatement[] {statement};
    }
}
