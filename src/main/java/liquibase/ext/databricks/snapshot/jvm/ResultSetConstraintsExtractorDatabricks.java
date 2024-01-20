package liquibase.ext.databricks.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;;
import liquibase.structure.core.Schema;

import java.sql.SQLException;
import java.util.List;
import liquibase.ext.databricks.snapshot.jvm.ResultSetCacheDatabricks;

public class ResultSetConstraintsExtractorDatabricks extends ResultSetCacheDatabricks.SingleResultSetExtractor {

    private final Database database;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public ResultSetConstraintsExtractorDatabricks(DatabaseSnapshot databaseSnapshot, String catalogName, String schemaName,
                                                  String tableName) {
        super(databaseSnapshot.getDatabase());
        this.database = databaseSnapshot.getDatabase();
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public boolean bulkContainsSchema(String schemaKey) {
        return false;
    }

    @Override
    public ResultSetCacheDatabricks.RowData rowKeyParameters(CachedRow row) {
        return new ResultSetCacheDatabricks.RowData(this.catalogName, this.schemaName, this.database,
                row.getString("TABLE_NAME"));
    }

    @Override
    public ResultSetCacheDatabricks.RowData wantedKeyParameters() {
        return new ResultSetCacheDatabricks.RowData(this.catalogName, this.schemaName, this.database, this.tableName);
    }

    @Override
    public List<CachedRow> fastFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(this.catalogName, this.schemaName)
                .customize(this.database);

        return executeAndExtract(this.database,
                createSql(((AbstractJdbcDatabase) this.database).getJdbcCatalogName(catalogAndSchema),
                        ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), this.tableName));
    }

    @Override
    public List<CachedRow> bulkFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(this.catalogName, this.schemaName)
                .customize(this.database);

        return executeAndExtract(this.database,
                createSql(((AbstractJdbcDatabase) this.database).getJdbcCatalogName(catalogAndSchema),
                        ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), null));
    }

    private String createSql(String catalog, String schema, String table) {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(catalog, schema).customize(this.database);

        String jdbcSchemaName = this.database.correctObjectName(
                ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), Schema.class);

        String sql = "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, TABLE_NAME FROM "
                + this.database.getSystemSchema() + ".TABLE_CONSTRAINTS " + " WHERE TABLE_SCHEMA='" + jdbcSchemaName
                + "' AND CONSTRAINT_TYPE='UNIQUE'";
        if (table != null) {
            sql += " AND TABLE_NAME='" + table + "'";
        }

        return sql;
    }
}