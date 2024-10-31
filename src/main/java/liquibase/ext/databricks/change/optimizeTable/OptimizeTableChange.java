package liquibase.ext.databricks.change.optimizeTable;


import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import liquibase.change.Change;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.ArrayList;

@Setter
@Getter
@DatabaseChange(name = "optimizeTable", description = "Optimize and ZOrder Table", priority =  PrioritizedService.PRIORITY_DATABASE)
public class OptimizeTableChange extends AbstractChange {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String zorderColumns;

    @Override
    public String getConfirmationMessage() {
        return MessageFormat.format("{0}.{1}.{2} successfully optimized.", getCatalogName(), getSchemaName(), getTableName());
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
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

        return new SqlStatement[] {statement};

    }
}
