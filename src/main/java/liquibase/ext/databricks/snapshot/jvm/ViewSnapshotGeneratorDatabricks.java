package liquibase.ext.databricks.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.databricks.database.DatabricksConnection;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.ViewSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.View;
import liquibase.util.StringUtil;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import liquibase.ext.databricks.database.DatabricksDatabase;

public class ViewSnapshotGeneratorDatabricks extends ViewSnapshotGenerator {


    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        int priority = super.getPriority(objectType, database);
        if (priority > PRIORITY_NONE && database instanceof DatabricksDatabase) {
            priority += DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
        }
        return priority;
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        if (((View) example).getDefinition() != null) {
            return example;
        } else {
            Database database = snapshot.getDatabase();
            Schema schema = example.getSchema();
            DatabaseConnection connection = database.getConnection();

            CatalogAndSchema catalogAndSchema = (new CatalogAndSchema(schema.getCatalogName(), schema.getName())).customize(database);
            String jdbcSchemaName = database.correctObjectName(((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema), Schema.class);
            String query = String.format("SELECT view_definition FROM %s.%s.VIEWS WHERE table_name='%s' AND table_schema='%s' AND table_catalog='%s';",
                    schema.getCatalogName(), database.getSystemSchema(), example.getName(), schema.getName(), schema.getCatalogName());

            // DEBUG
            //System.out.println("Snapshot Database Connection URL : " + database.getConnection().getURL());
            //System.out.println("Snapshot Database Connection Class : " + database.getConnection().getClass().getName());


            List<Map<String, ?>> viewsMetadataRs = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                    .getExecutor("jdbc", database).queryForList(new RawSqlStatement(query));

            // New Code, likely superfluous, was used for testing
            /// This should use our existing DatabaseConnection url processing
            String rawViewDefinition = null;

            try (ResultSet viewMetadataResultSet = ((DatabricksConnection) connection).createStatement().executeQuery(query)) {
                //System.out.println("Raw Result VIEW " + viewMetadataResultSet);

                viewMetadataResultSet.next();
                rawViewDefinition = viewMetadataResultSet.getString(1);


            } catch (Exception e) {
                Scope.getCurrentScope().getLog(getClass()).info("Error getting View Definiton via existing context, going to pull from URL", e);
            }

            /// Old Code

            if (viewsMetadataRs.isEmpty()) {
                return null;
            } else {

                Map<String, ?> row = viewsMetadataRs.get(0);
                String rawViewName = example.getName();
                String rawSchemaName = schema.getName();
                String rawCatalogName = schema.getCatalogName();


                View view = (new View()).setName(this.cleanNameFromDatabase(rawViewName, database));
                CatalogAndSchema schemaFromJdbcInfo = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(rawCatalogName, rawSchemaName);
                view.setSchema(new Schema(schemaFromJdbcInfo.getCatalogName(), schemaFromJdbcInfo.getSchemaName()));

                String definition = rawViewDefinition;

                if (definition == null || definition.isEmpty()) {
                    definition = (String) row.get("view_definition");

                }

                int length = definition.length();
                if (length > 0 && definition.charAt(length - 1) == 0) {
                    definition = definition.substring(0, length - 1);
                }

                definition = StringUtil.trimToNull(definition);
                if (definition == null) {
                    definition = "[CANNOT READ VIEW DEFINITION]";
                }

                view.setDefinition(definition);

                return view;
            }

        }
    }
}
