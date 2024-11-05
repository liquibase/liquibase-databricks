package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.Change;
import liquibase.change.core.CreateViewChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.MissingViewChangeGenerator;
import liquibase.ext.databricks.change.createView.CreateViewChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.View;

import static liquibase.ext.databricks.diff.output.changelog.ChangedTblPropertiesUtil.getExtendedProperties;

/**
 * Custom implementation of {@link MissingViewChangeGenerator} for Databricks.
 */
public class MissingViewChangeGeneratorDatabricks extends MissingViewChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && View.class.isAssignableFrom(objectType)) {
            return PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        Change[] changes = super.fixMissing(missingObject, control, referenceDatabase, comparisonDatabase, chain);
        if (changes == null || changes.length == 0) {
            return changes;
        }
        changes[0] = getCreateViewChangeDatabricks(getExtendedProperties(missingObject.getAttribute("tblProperties", String.class)), changes);
        return changes;
    }

    private CreateViewChangeDatabricks getCreateViewChangeDatabricks(String tblProperties, Change[] changes) {
        CreateViewChange temp = (CreateViewChange) changes[0];
        CreateViewChangeDatabricks createViewChangeDatabricks = new CreateViewChangeDatabricks();
        createViewChangeDatabricks.setViewName(temp.getViewName());
        createViewChangeDatabricks.setSelectQuery(temp.getSelectQuery());
        createViewChangeDatabricks.setReplaceIfExists(temp.getReplaceIfExists());
        createViewChangeDatabricks.setSchemaName(temp.getSchemaName());
        createViewChangeDatabricks.setCatalogName(temp.getCatalogName());
        createViewChangeDatabricks.setRemarks(temp.getRemarks());
        createViewChangeDatabricks.setFullDefinition(temp.getFullDefinition());
        createViewChangeDatabricks.setPath(temp.getPath());
        createViewChangeDatabricks.setRelativeToChangelogFile(temp.getRelativeToChangelogFile());
        createViewChangeDatabricks.setEncoding(temp.getEncoding());
        createViewChangeDatabricks.setTblProperties(tblProperties);
        return createViewChangeDatabricks;
    }

    @Override
    protected CreateViewChange createViewChange() {
        return new CreateViewChangeDatabricks();
    }
}
