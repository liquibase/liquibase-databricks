package liquibase.ext.databricks.snapshot.jvm;

import liquibase.snapshot.ResultSetCache;
import liquibase.database.Database;

public class ResultSetCacheDatabricks extends ResultSetCache {
    public static class RowData extends ResultSetCache.RowData {
        public RowData(String catalog, String schema, Database database, String... parameters) {
            super(catalog, schema, database, parameters);
        }
    }

    public abstract static class SingleResultSetExtractor extends ResultSetCache.SingleResultSetExtractor {
        protected SingleResultSetExtractor(Database database) {
            super(database);
        }
    }
}