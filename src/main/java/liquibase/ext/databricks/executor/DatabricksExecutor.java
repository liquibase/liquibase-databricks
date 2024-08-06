package liquibase.ext.databricks.executor;


import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;
import java.util.List;

public class DatabricksExecutor extends JdbcExecutor {

    @Override
    public int getPriority() {
        return PrioritizedService.PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public void execute(SqlStatement sql) throws DatabaseException {
        super.execute(sql);
    }

    @Override
    public void execute(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        super.execute(sql, sqlVisitors);
    }
}