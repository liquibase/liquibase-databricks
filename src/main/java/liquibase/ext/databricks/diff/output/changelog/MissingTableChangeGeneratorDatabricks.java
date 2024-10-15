package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.Change;
import liquibase.change.core.CreateTableChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.MissingTableChangeGenerator;
import liquibase.ext.databricks.change.createTable.CreateTableChangeDatabricks;
import liquibase.ext.databricks.change.createTable.ExtendedTableProperties;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MissingTableChangeGeneratorDatabricks extends MissingTableChangeGenerator {

    private static final String CLUSTERING_INFORMATION_TBL_PROPERTY_START = "clusteringColumns=[[";

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && Table.class.isAssignableFrom(objectType)) {
            return PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase,
                               ChangeGeneratorChain chain) {
        Change[] changes = super.fixMissing(missingObject, control, referenceDatabase, comparisonDatabase, chain);
        if (changes == null || changes.length == 0) {
            return changes;
        }
        ExtendedTableProperties extendedTableProperties = new ExtendedTableProperties(
                missingObject.getAttribute("Location", String.class),
                missingObject.getAttribute("tblProperties", String.class));

        changes[0] = getCreateTableChangeDatabricks(extendedTableProperties, changes);
        return changes;
    }

    private CreateTableChangeDatabricks getCreateTableChangeDatabricks(ExtendedTableProperties extendedTableProperties, Change[] changes) {
        CreateTableChange temp = (CreateTableChange) changes[0];
        CreateTableChangeDatabricks createTableChangeDatabricks = new CreateTableChangeDatabricks();
        createTableChangeDatabricks.setColumns(temp.getColumns());
        createTableChangeDatabricks.setTableType(temp.getTableType());
        createTableChangeDatabricks.setCatalogName(temp.getCatalogName());
        createTableChangeDatabricks.setSchemaName(temp.getSchemaName());
        createTableChangeDatabricks.setTableName(temp.getTableName());
        createTableChangeDatabricks.setTablespace(temp.getTablespace());
        createTableChangeDatabricks.setRemarks(temp.getRemarks());
        createTableChangeDatabricks.setIfNotExists(temp.getIfNotExists());
        createTableChangeDatabricks.setRowDependencies(temp.getRowDependencies());
        if(extendedTableProperties.getTblProperties().contains(CLUSTERING_INFORMATION_TBL_PROPERTY_START)) {
            //This approach should be rewritten after implementing tblProperties map parsing to Map struct collection
            String tblProperties = extendedTableProperties.getTblProperties();
            createTableChangeDatabricks.setClusterColumns(parseClusterColumns(tblProperties));
            //Remove clusteringColumns property from tblProperties
            extendedTableProperties.setTblProperties(tblProperties.replace(getClusteringColumnsSubstring(tblProperties), ""));
        }

        createTableChangeDatabricks.setExtendedTableProperties(extendedTableProperties);
        return createTableChangeDatabricks;
    }

    @Override
    protected CreateTableChange createCreateTableChange() {
        return new CreateTableChangeDatabricks();
    }

    /**
     * There might be 2 edge cases with table properties map:
     *      <ul><li>User specified a custom tblProperty that appears prior databricks internal managed
     *          clusteringColumns property and this custom tblProperty contains string snippet 'clusteringColumns=[['.
     *          Pattern and matcher would process properties incorrect if there was present structure ["anyString"]
     *          in between string snippet 'clusteringColumns=[[' and actual databricks managed property
     *          clusteringColumns.
     *      <li>User specified a custom table property that contains string snippet 'clusteringColumns=' and there are
     *          no clustering configured on the table.<ul/>
     * @param  tblProperties
     *         The tblProperties map in a raw string format that returns as part of result set of
     *         DESCRIBE TABLE EXTENDED query.
     * @return Coma separated clustering columns extracted from tblProperties
     * */
    private String parseClusterColumns(String tblProperties) {
        //  Actual pattern - "\[\"([^\"]*)\"\]"
        Pattern pattern = Pattern.compile("\\[\\\"([^\\\"]*)\\\"\\]");
        String clusteringColumnsRaw = getClusteringColumnsSubstring(tblProperties);
        StringBuilder clusterColumnNames = new StringBuilder();
        try{
            Matcher matcher = pattern.matcher(clusteringColumnsRaw);
            for (int i = 0; matcher.find(); i++) {
                //getting first matching group to avoid quotes and brackets
                String group = matcher.group(1);
                clusterColumnNames.append(i == 0 ? "" : ",").append(group);
            }
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }

        return clusterColumnNames.toString();
    }

    private String getClusteringColumnsSubstring(String tblProperties) {
        int clusterColumnsStartIndex = tblProperties.indexOf(CLUSTERING_INFORMATION_TBL_PROPERTY_START);
        // To avoid appearance anywhere before clusteringColumns property we are specifying clusterColumnsStartIndex
        // to start search from for end index snippet.
        return tblProperties.substring(clusterColumnsStartIndex, tblProperties.indexOf("\"]],", clusterColumnsStartIndex) + 4);
    }
}