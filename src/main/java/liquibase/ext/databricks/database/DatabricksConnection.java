package liquibase.ext.databricks.database;

import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import java.sql.Connection;

public class DatabricksConnection extends JdbcConnection {

    public DatabricksConnection(Connection connection) {
        super(connection);
    }
    @Override
    public void setAutoCommit(boolean autoCommit) throws DatabaseException {
        // no-op for Databricks since there is not a concept of committing

    }
}