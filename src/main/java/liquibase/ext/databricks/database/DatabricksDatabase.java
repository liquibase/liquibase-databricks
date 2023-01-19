package liquibase.ext.databricks.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.exception.DatabaseException;
import org.jetbrains.annotations.NotNull;

public class DatabricksDatabase extends AbstractJdbcDatabase {

    private static final String PRODUCT_NAME = "Databricks";
    public static final int PRIORITY_DEFAULT = 9999;
    public static final int DEFAULT_PORT = 15001;
    private static final String DEFAULT_DRIVER = "com.databricks.client.jdbc.Driver" ;

    public DatabricksDatabase() {

    }

    @Override
    public String getShortName() {
        return "databricks";
    }

    @Override
    public Integer getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(liquibase.database.@NotNull DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equals(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        return DEFAULT_DRIVER;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

}