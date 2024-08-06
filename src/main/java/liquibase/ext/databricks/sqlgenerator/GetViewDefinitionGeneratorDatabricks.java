package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.sqlgenerator.core.GetViewDefinitionGenerator;

/**
 * Databricks-specific view definition generator.
 * Uses Databricks-specific query to read full view definition statement from a database.
 */

public class GetViewDefinitionGeneratorDatabricks extends GetViewDefinitionGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public boolean supports(GetViewDefinitionStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public Sql[] generateSql(GetViewDefinitionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        CatalogAndSchema schema = new CatalogAndSchema(statement.getCatalogName(), statement.getSchemaName()).customize(database);
        // We can use non quoted schema/catalog/view names here.
        // SELECT GET_DDL('VIEW', 'TEST.BAD$SCHEMA_NAME.BAD$%^VIEW_NAME', TRUE) - works fine.
        // "TRUE" means that the returned result will be in the full representation
        return new Sql[] {
                new UnparsedSql( "SELECT view_definition FROM "
                        + schema.getCatalogName() + ".INFORMATION_SCHEMA.VIEWS" + " WHERE table_catalog = '" + statement.getCatalogName() + "' "
                        + "AND table_schema = '" + statement.getSchemaName() + "' "
                        + "AND table_name = '" + statement.getViewName() + "'"
                )
        };
    }
}