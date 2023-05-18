package liquibase.ext.databricks.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.structure.DatabaseObject;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.util.StringUtil;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Collections;


public class DatabricksDatabase extends AbstractJdbcDatabase {

    // define env variables for database
    public static final String PRODUCT_NAME = "databricks";
    // Set default catalog - must be unity Catalog Enabled

    // This is from the new INFORMATION_SCHEMA() database
    private Set<String> systemTablesAndViews = new HashSet<>();

    //Define data type names enabled for auto-increment columns - currently only BIGINT
    public static final List<String> VALID_AUTO_INCREMENT_COLUMN_TYPE_NAMES = Collections.unmodifiableList(Arrays.asList("BIGINT"));

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
    public String getShortName() {
        return "databricks";
    }

    @Override
    public String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Set<String> getSystemViews() {
        return systemTablesAndViews;
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
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName()) || conn.getDatabaseProductName().equalsIgnoreCase("SparkSQL");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:databricks:") || url.startsWith("jdbc:spark:")) {
            return "com.databricks.client.jdbc.Driver";
        }
        return null;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
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
    protected String getAutoIncrementClause(final String generationType, final Boolean defaultOnNull) {

        if (StringUtil.isEmpty(generationType)) {
            return super.getAutoIncrementClause();
        }

        String autoIncrementClause = "GENERATED %s AS IDENTITY"; // %s -- [ ALWAYS | BY DEFAULT ]
        return String.format(autoIncrementClause, generationType);
    }

    @Override
    protected String getAutoIncrementStartWithClause() {
        return "%d";
    }

    @Override
    protected String getAutoIncrementByClause() {
        return "%d";
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
}