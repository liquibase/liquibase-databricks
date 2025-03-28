package liquibase.ext.databricks.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ViewSnapshotGenerator;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.View;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Overrides ViewSnapshotGenerator for Databricks views contemplating the tblProperties field
 */
public class ViewSnapshotGeneratorDatabricks extends ViewSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        return database instanceof DatabricksDatabase ? PRIORITY_DATABASE : PRIORITY_NONE;
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        if (((View) example).getDefinition() != null) {
            return example;
        } else {
            Database database = snapshot.getDatabase();
            Schema schema = example.getSchema();

            String query = String.format("SELECT view_definition FROM %s.%s.VIEWS WHERE table_name=? AND table_schema=? AND table_catalog=?",
                    schema.getCatalogName(), database.getSystemSchema());

            List<Map<String, ?>> viewsMetadataRs = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                    .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query, example.getName(), schema.getName(), schema.getCatalogName()));

            if (viewsMetadataRs.isEmpty()) {
                return null;
            } else {
                String viewName = this.cleanNameFromDatabase(example.getName(), database);
                String rawSchemaName = schema.getName();
                String rawCatalogName = schema.getCatalogName();

                CatalogAndSchema schemaFromJdbcInfo = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(rawCatalogName, rawSchemaName);
                View view = (View) new View()
                        .setName(viewName)
                        .setSchema(new Schema(schemaFromJdbcInfo.getCatalogName(), schemaFromJdbcInfo.getSchemaName()))
                        .setAttribute("tblProperties", this.getTblProperties(database, viewName));
                view.setDefinition(getViewDefinition(viewsMetadataRs));

                return view;
            }
        }
    }

    private String getViewDefinition(List<Map<String, ?>> viewsMetadataRs) {
        Map<String, ?> row = viewsMetadataRs.get(0);
        String definition = (String) row.get("VIEW_DEFINITION");

        int length = definition.length();
        if (length > 0 && definition.charAt(length - 1) == 0) {
            definition = definition.substring(0, length - 1);
        }

        definition = StringUtils.trimToNull(definition);
        if (definition == null) {
            definition = "[CANNOT READ VIEW DEFINITION]";
        }
        return definition;
    }

    private String getTblProperties(Database database, String viewName) throws DatabaseException {
        String query = String.format("SHOW TBLPROPERTIES %s.%s.%s;", database.getDefaultCatalogName(), database.getDefaultSchemaName(), viewName);
        List<Map<String, ?>> tablePropertiesResponse = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawParameterizedSqlStatement(query));

        StringBuilder csvString = new StringBuilder();
        tablePropertiesResponse.forEach(tableProperty ->
            csvString.append("'").append(tableProperty.get("KEY")).append("'='").append(tableProperty.get("VALUE")).append("', ")
        );
        return csvString.toString().replaceAll(", $", "");
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{ViewSnapshotGenerator.class};
    }
}
