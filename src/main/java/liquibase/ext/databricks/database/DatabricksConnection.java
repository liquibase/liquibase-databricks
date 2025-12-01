package liquibase.ext.databricks.database;

import com.databricks.client.jdbc.jdbc42.S42Connection;
import com.databricks.client.spark.core.SparkJDBCConnection;
import liquibase.Scope;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class DatabricksConnection extends JdbcConnection {

    private S42Connection con;
    public DatabricksConnection() {}

    public DatabricksConnection(Connection conn) {
        this.con = (S42Connection) conn;
    }

    @Override
    public String getDatabaseProductName() throws DatabaseException {
        try {
            return this.getWrappedConnection().getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Connection getWrappedConnection() {
        return con;
    }

    public SparkJDBCConnection getUnderlyingSparkConnection() {
        if (con.getConnection() instanceof SparkJDBCConnection) {
            return (SparkJDBCConnection) con.getConnection();
        }
        return null;
    }

    @Override
    public Connection getUnderlyingConnection() {
        return con;
    }

    @Override
    public void open(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {

        driverProperties.setProperty("UserAgentEntry", "Liquibase");
        driverProperties.setProperty("EnableArrow", "0");
        // Set UserAgent to specify to Databricks that liquibase is the tool running these commands
        // Set EnableArrow because the arrow results break everything. And the JDBC release notes say to just disable it.

        // Ensure there's a terminating semicolon for consistent parsing
        if (!url.endsWith(";")) {
            url += ";";
        }

        // Only append parameters that don't already exist in the URL to avoid duplicate key errors
        StringBuilder urlBuilder = new StringBuilder(url);
        if (!urlContainsParam(url, "UserAgentEntry")) {
            urlBuilder.append("UserAgentEntry=Liquibase;");
        }
        if (!urlContainsParam(url, "EnableArrow")) {
            urlBuilder.append("EnableArrow=0;");
        }

        String updatedUrl = urlBuilder.toString();

        this.openConn(updatedUrl, driverObject, driverProperties);
    }

    public void openConn(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
        String sanitizedUrl = sanitizeUrl(url);
        try {
            Scope.getCurrentScope().getLog(this.getClass()).info("opening connection " + sanitizedUrl);
            this.con = (S42Connection) driverObject.connect(url, driverProperties);
            if (this.con == null) {
                Scope.getCurrentScope().getLog(this.getClass()).severe("Connection could not be created");
                throw new DatabaseException("Connection could not be created to " + sanitizedUrl + " with driver " + driverObject.getClass().getName() + ".  Possibly the wrong driver for the given database URL");
            }
        } catch (SQLException sqle) {
            throw new DatabaseException("Connection could not be created to " + sanitizedUrl + " with driver " + driverObject.getClass().getName() + ".  " + sqle.getMessage());
        }
    }

    @Override
    public boolean supports(String url) {
        return url.toLowerCase().contains("databricks");
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE + 101;
    }

    @Override
    public boolean getAutoCommit() throws DatabaseException {
        return true;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws DatabaseException {
    }

    protected static String getUrlParamValue(String url, String paramName, String defaultValue) {

        //System.out.println("PARSE URL - url" + url);

        if (url == null) {
            return null;
        }
        // Ensure there's a terminating semicolon for consistent parsing
        if (!url.endsWith(";")) {
            url += ";";
        }
        // Remove spaces and split by semicolon
        String[] uriArgs = url.replace(" ", "").split(";");

       // System.out.println("PARSE URL - url args" + uriArgs.toString());

        // Use Java Streams to find the parameter value
        Optional<String> paramString = Arrays.stream(uriArgs)
                .filter(x -> x.startsWith(paramName + "="))
                .findFirst();
        // Return the parameter value if found, otherwise return the default value
        if (!paramString.isPresent()) {
            return defaultValue;
        }
        String[] defaultParamsArr = paramString.get().split("=");
        return defaultParamsArr.length > 1 ? defaultParamsArr[1] : defaultValue; // Check to avoid index out of bound
    }

    /**
     * Check if a parameter exists in the URL (case-insensitive) to avoid duplicate key errors
     */
    protected static boolean urlContainsParam(String url, String paramName) {
        if (url == null || paramName == null) {
            return false;
        }
        // Ensure there's a terminating semicolon for consistent parsing
        String normalizedUrl = url;
        if (!normalizedUrl.endsWith(";")) {
            normalizedUrl += ";";
        }
        // Remove spaces and split by semicolon
        String[] uriArgs = normalizedUrl.replace(" ", "").split(";");
        
        // Use case-insensitive matching to find the parameter
        String lowerParamName = paramName.toLowerCase();
        return Arrays.stream(uriArgs)
                .anyMatch(x -> {
                    int equalsIndex = x.indexOf('=');
                    if (equalsIndex == -1) return false;
                    String paramPart = x.substring(0, equalsIndex).toLowerCase();
                    return paramPart.equals(lowerParamName);
                });
    }


    @Override
    public String getDatabaseProductVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getDatabaseMajorVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseMinorVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /////////////////////////////////////////////////// copy from parent ///////////////////////////////////////////////////
    @Override
    protected String getConnectionUrl() throws SQLException {

        String rawUrl = con.getMetaData().getURL();
        
        // Handle null or empty URL according to JDBC spec
        if (rawUrl == null) {
            rawUrl = "";
        }
        
        // Check for ; characters
        StringBuilder urlBuilder = new StringBuilder(rawUrl);
        
        // Ensure there's a terminating semicolon for consistent parsing
        if (rawUrl.isEmpty() || rawUrl.charAt(rawUrl.length() - 1) != ';') {
            urlBuilder.append(";");
        }
        
        // Only append parameters that don't already exist in the URL to avoid duplicate key errors
        if (!urlContainsParam(rawUrl, "UserAgentEntry")) {
            urlBuilder.append("UserAgentEntry=Liquibase;");
        }
        if (!urlContainsParam(rawUrl, "EnableArrow")) {
            urlBuilder.append("EnableArrow=0;");
        }
        
        return urlBuilder.toString();
    }

    @Override
    public String getConnectionUserName() {
        try {
            return con.getMetaData().getUserName();
        } catch (SQLException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    @Override
    public void clearWarnings() throws DatabaseException {
        try {
            con.clearWarnings();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() throws DatabaseException {
        rollback();
        try {
            con.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void commit() throws DatabaseException {
        try {
            if (!con.getAutoCommit()) {
                con.commit();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Statement createStatement() throws DatabaseException {
        try {
            return con.createStatement();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.createStatement(resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws DatabaseException {
        try {
            return con.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String getCatalog() throws DatabaseException {
        try {
            return con.getCatalog();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setCatalog(String catalog) throws DatabaseException {
        try {
            con.setCatalog(catalog);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getHoldability() throws DatabaseException {
        try {
            return con.getHoldability();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setHoldability(int holdability) throws DatabaseException {
        try {
            con.setHoldability(holdability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws DatabaseException {
        try {
            return con.getMetaData();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getTransactionIsolation() throws DatabaseException {
        try {
            return con.getTransactionIsolation();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws DatabaseException {
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws DatabaseException {
        try {
            return con.getTypeMap();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws DatabaseException {
        try {
            con.setTypeMap(map);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws DatabaseException {
        try {
            return con.getWarnings();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean isClosed() throws DatabaseException {
        return con.isClosed();
    }

    @Override
    public boolean isReadOnly() throws DatabaseException {
        try {
            return con.isReadOnly();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws DatabaseException {
        try {
            con.setReadOnly(readOnly);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String nativeSQL(String sql) throws DatabaseException {
        try {
            return con.nativeSQL(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.prepareCall(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws DatabaseException {
        try {
            return con.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws DatabaseException {
        try {
            return con.prepareCall(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws DatabaseException {
        try {
            return con.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, columnIndexes);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, columnNames);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws DatabaseException {
        try {
            return con.prepareStatement(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws DatabaseException {
        try {
            con.releaseSavepoint(savepoint);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void rollback() throws DatabaseException {
        try {
            if (!con.getAutoCommit() && !con.isClosed()) {
                con.rollback();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws DatabaseException {
        try {
            if (!con.getAutoCommit()) {
                con.rollback(savepoint);
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Savepoint setSavepoint() throws DatabaseException {
        try {
            return con.setSavepoint();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws DatabaseException {
        try {
            return con.setSavepoint(name);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof JdbcConnection)) {
            return false;
        }
        Connection underlyingConnection = this.getUnderlyingConnection();
        if (underlyingConnection == null) {
            return ((JdbcConnection) obj).getUnderlyingConnection() == null;
        }

        return underlyingConnection.equals(((JdbcConnection) obj).getUnderlyingConnection());
    }

    @Override
    public int hashCode() {
        Connection underlyingConnection = this.getUnderlyingConnection();
        try {
            if ((underlyingConnection == null) || underlyingConnection.isClosed()) {
                return super.hashCode();
            }
        } catch (SQLException e) {
            return super.hashCode();
        }
        return underlyingConnection.hashCode();
    }

}
