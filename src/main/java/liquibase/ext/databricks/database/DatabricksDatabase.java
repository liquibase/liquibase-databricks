package liquibase.ext.databricks.database;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;
import lombok.Setter;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class DatabricksDatabase extends AbstractJdbcDatabase {

    // define env variables for database
    public static final String PRODUCT_NAME = "databricks";
    // Set default catalog - must be unity Catalog Enabled

    @Setter
    private String systemSchema = "information_schema";

    // This is from the new INFORMATION_SCHEMA() database
    private Set<String> systemTablesAndViews = new HashSet<>();

    //Define data type names enabled for auto-increment columns - currently only BIGINT
    public static final List<String> VALID_AUTO_INCREMENT_COLUMN_TYPE_NAMES = Collections.singletonList("BIGINT");

    public DatabricksDatabase() {
        super.setCurrentDateTimeFunction("current_timestamp()");
        super.addReservedWords(getDatabricksReservedWords());
        super.defaultAutoIncrementStartWith = BigInteger.ONE;
        super.defaultAutoIncrementBy = BigInteger.ONE;
    }

    @Override
    protected String getQuotingStartCharacter() {
        return "`";
    }

    @Override
    protected String getQuotingEndCharacter() {
        return "`";
    }

    @Override
    protected String getQuotingEndReplacement() {
        return "``";
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        return super.getDatabaseChangeLogTableName().toLowerCase(Locale.US);
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        return super.getDatabaseChangeLogLockTableName().toLowerCase(Locale.US);
    }

    @Override
    public String getShortName() {
        return PRODUCT_NAME;
    }

    @Override
    public String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Set<String> getSystemViews() {
        return systemTablesAndViews;
    }

    // Static Value
    @Override
    public String getSystemSchema() {
        return this.systemSchema;
    }

    @Override
    public Integer getDefaultPort() {
        return 443;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName()) || conn.getDatabaseProductName().equalsIgnoreCase("SparkSQL") || conn.getDatabaseProductName().equalsIgnoreCase("spark");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:databricks") || url.startsWith("jdbc:spark")) {

            return "com.databricks.client.jdbc.Driver";
        }
        return null;
    }


    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return true;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return true;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSequences() { return false; }

    @Override
    public boolean supportsDatabaseChangeLogHistory() {
        return true;
    }

    @Override
    public String getAutoIncrementClause(final BigInteger startWith, final BigInteger incrementBy, final String generationType, final Boolean defaultOnNull) {
        if (!this.supportsAutoIncrement()) {
            return "";
        }

        // generate an SQL:2003 STANDARD compliant auto increment clause by default
        String autoIncrementClause = getAutoIncrementClause(generationType, defaultOnNull);

        boolean generateStartWith = generateAutoIncrementStartWith(startWith);
        boolean generateIncrementBy = generateAutoIncrementBy(incrementBy);

        if (generateStartWith || generateIncrementBy) {
            autoIncrementClause += buildAutoIncrementClause(startWith, incrementBy, generateStartWith, generateIncrementBy);
        }

        return autoIncrementClause;
    }

    private String buildAutoIncrementClause(final BigInteger startWith, final BigInteger incrementBy, boolean generateStartWith, boolean generateIncrementBy) {
        StringBuilder clauseBuilder = new StringBuilder(getAutoIncrementOpening());

        if (generateStartWith) {
            clauseBuilder.append(String.format(getAutoIncrementStartWithClause(), (startWith == null) ? defaultAutoIncrementStartWith : startWith));
        }

        if (generateIncrementBy) {
            if (generateStartWith) { // for databricks there is no comma
                clauseBuilder.append(" ");
            }
            clauseBuilder.append(String.format(getAutoIncrementByClause(), (incrementBy == null) ? defaultAutoIncrementBy : incrementBy));
        }

        clauseBuilder.append(getAutoIncrementClosing());
        return clauseBuilder.toString();
    }

    @Override
    public boolean generateAutoIncrementStartWith(BigInteger startWith) {
        return true;
    }

    @Override
    public boolean generateAutoIncrementBy(BigInteger incrementBy) {
        return true;
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawCallStatement("select current_schema()");
    }

    @Override
    protected String getConnectionSchemaName() {
        DatabaseConnection connection = getConnection();

        if (connection == null) {
            return null;
        }

        try (ResultSet resultSet = ((DatabricksConnection) connection).createStatement().executeQuery("SELECT CURRENT_SCHEMA()")) {
            resultSet.next();
            return resultSet.getString(1);

        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error getting default schema via existing context, going to pull from URL", e);
        }

        try {

            String foundSchema = parseUrlForSchema(connection.getURL());
            Scope.getCurrentScope().getLog(getClass()).info("SCHEMA IDENTIFIED: " + foundSchema);

            return foundSchema;
        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).warning("Cannot get default / defined schema from URL or current session.");
        }
        // Return null, not default to force user to supply the schema
        return null;

    }

    @Override
    protected String getConnectionCatalogName() {
        DatabaseConnection connection = getConnection();

        if (connection == null) {
            return null;
        }

        try{
            return connection.getCatalog();
        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).warning("Cannot get default / defined CATALOG from current session.");
        }

        try (ResultSet resultSet = ((DatabricksConnection) connection).createStatement().executeQuery("SELECT CURRENT_CATALOG()")) {
            resultSet.next();
            return resultSet.getString(1);

        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error getting default catalog via existing context, going to pull from URL", e);
        }

        try {
            String foundCatalog = parseUrlForCatalog(connection.getURL());
            Scope.getCurrentScope().getLog(getClass()).info("CATALOG IDENTIFIED: " + foundCatalog);

            return foundCatalog;

        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).warning("Cannot get default / defined CATALOG from URL");
        }
        // Return null, not default to force user to supply the catalog
        return null;

    }

    private String parseUrlForSchema(String url) {
        String schemaToken = "ConnSchema";
        return DatabricksConnection.getUrlParamValue(url, schemaToken, this.defaultSchemaName);
    }

    private String parseUrlForCatalog(String url) {
        String schemaToken = "ConnCatalog";
        return  DatabricksConnection.getUrlParamValue(url, schemaToken, this.defaultCatalogName);
    }

    @Override
    public void setDefaultSchemaName(final String schemaName) {
        this.defaultSchemaName = correctObjectName(schemaName, Schema.class);
    }

    @Override
    public void setDefaultCatalogName(final String catalogName) {
        this.defaultCatalogName = correctObjectName(catalogName, Catalog.class);
    }

    private Set<String> getDatabricksReservedWords() {

        // Get Reserved words from: https://docs.databricks.com/sql/language-manual/sql-ref-reserved-words.html
        return new HashSet<>(Arrays.asList(
                "ANTI",
                "CROSS",
                "EXCEPT",
                "FULL",
                "INNER",
                "INTERSECT",
                "JOIN",
                "LATERAL",
                "LEFT",
                "MINUS",
                "NATURAL",
                "ON",
                "RIGHT",
                "SEMI",
                "USING",
                "UNION",
                "NULL",
                "DEFAULT",
                "TRUE",
                "FALSE",
                "LATERAL",
                "BUILTIN",
                "SESSION",
                "INFORMATION_SCHEMA",
                "SYS",
                "ALL",
                "ALTER",
                "AND",
                "ANY",
                "ARRAY",
                "AS",
                "AT",
                "AUTHORIZATION",
                "BETWEEN", "BOTH", "BY",
                "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONSTRAINT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
                "DELETE", "DESCRIBE", "DISTINCT", "DROP",
                "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "EXTERNAL", "EXTRACT",
                "FETCH", "FILTER", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION",
                "GLOBAL", "GRANT", "GROUP", "GROUPING",
                "HAVING",
                "IN", "INNER", "INSERT", "INTERSECT", "INTERVAL", "INTO", "IS",
                "JOIN",
                "LEADING", "LEFT", "LIKE", "LOCAL",
                "NATURAL", "NO", "NOT", "NULL",
                "OF", "ON", "ONLY", "OR", "ORDER", "OUT", "OUTER", "OVERLAPS",
                "PARTITION", "POSITION", "PRIMARY",
                "RANGE", "REFERENCES", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS",
                "SELECT", "SESSION_USER", "SET", "SOME", "START",
                "TABLE", "TABLESAMPLE", "THEN", "TIME", "TO", "TRAILING", "TRUE", "TRUNCATE",
                "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "USER", "USING",
                "VALUES",
                "WHEN", "WHERE", "WINDOW", "WITH"
        ));
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        DatabaseConnection dbConn;
        if (conn instanceof JdbcConnection) {
            // (see Databricks Connection for details)
            dbConn = new DatabricksConnection(((JdbcConnection) conn).getWrappedConnection());
        } else {
            dbConn = conn;
        }
        super.setConnection(dbConn);
    }

    @Override
    public void checkDatabaseConnection() throws DatabaseException {
        DatabricksConnection connection = (DatabricksConnection) getConnection();
        try {
            String catalogName = getConnectionCatalogName();
            String schemaName = getConnectionSchemaName();
            ResultSet schemasAlikeUsed = connection.getMetaData().getSchemas(catalogName, schemaName);
            while (schemasAlikeUsed.next()) {
                if (schemasAlikeUsed.getString(1).equals(schemaName)) {
                    return;
                }
            }
            throw new DatabaseException(String.format("Please specify existing schema and catalog in connection url. " +
                    "Current connection points to '%s.%s'", catalogName, schemaName));
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error checking database connection", e);
        }
    }

    @Override
    public String escapeStringForDatabase(String string) {
        if (string == null) {
            return null;
        }
        return string.replaceAll("((?<!\\\\)')", "\\\\'");
    }

}
