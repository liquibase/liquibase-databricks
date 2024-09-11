package liquibase.ext.databricks.sqlgenerator;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.ext.databricks.change.createView.CreateViewStatementDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.parser.LiquibaseSqlParser;
import liquibase.parser.SqlParserFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateViewGenerator;
import liquibase.statement.core.CreateViewStatement;
import liquibase.util.StringClauses;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class CreateViewGeneratorDatabricks extends CreateViewGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateViewStatement statement, Database database) {
        return super.supports(statement, database) && (database instanceof DatabricksDatabase);
    }

    @Override
    public Sql[] generateSql(CreateViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> sql = new ArrayList<>();

        SqlParserFactory sqlParserFactory = Scope.getCurrentScope().getSingleton(SqlParserFactory.class);
        LiquibaseSqlParser sqlParser = sqlParserFactory.getSqlParser();
        StringClauses viewDefinition = sqlParser.parse(statement.getSelectQuery(), true, true);

        if (!statement.isFullDefinition()) {
            viewDefinition
                    .prepend(" ")
                    .prepend("AS");
            addTblProperties(statement, viewDefinition);
            viewDefinition
                    .prepend(" ")
                    .prepend(database.escapeViewName(
                            statement.getCatalogName(), statement.getSchemaName(), statement.getViewName()))
                    .prepend(" ")
                    .prepend("VIEW")
                    .prepend(" ")
                    .prepend("CREATE");
        }

        if (statement.isReplaceIfExists() && !statement.getSelectQuery().toUpperCase().contains("OR REPLACE")) {
            viewDefinition.replace("CREATE", "CREATE OR REPLACE");
        }

        sql.add(new UnparsedSql(viewDefinition.toString(), getAffectedView(statement)));
        return sql.toArray(EMPTY_SQL);
    }

    private void addTblProperties(CreateViewStatement statement, StringClauses viewDefinition) {
        if (statement instanceof CreateViewStatementDatabricks) {
            CreateViewStatementDatabricks thisStatement = (CreateViewStatementDatabricks) statement;

            if (StringUtils.isNotEmpty(thisStatement.getTblProperties()) && !statement.getSelectQuery().toUpperCase().contains("TBLPROPERTIES")) {
                viewDefinition.prepend(" ")
                        .prepend("TBLPROPERTIES (" + thisStatement.getTblProperties() + ")");
            }
        }
    }
}
