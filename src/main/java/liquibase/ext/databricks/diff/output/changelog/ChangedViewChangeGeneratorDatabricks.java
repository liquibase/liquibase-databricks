package liquibase.ext.databricks.diff.output.changelog;

import liquibase.change.Change;
import liquibase.database.Database;
import liquibase.diff.Difference;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.core.ChangedViewChangeGenerator;
import liquibase.ext.databricks.change.alterViewProperties.AlterViewPropertiesChangeDatabricks;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.View;

import java.util.Arrays;

import static liquibase.ext.databricks.diff.output.changelog.ChangedTblPropertiesUtil.getAlterViewPropertiesChangeDatabricks;


public class ChangedViewChangeGeneratorDatabricks extends ChangedViewChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof DatabricksDatabase && super.getPriority(objectType, database) > PRIORITY_NONE) {
            return PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Change[] fixChanged(DatabaseObject changedObject, ObjectDifferences differences, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        Change[] changes = super.fixChanged(changedObject, differences, control, referenceDatabase, comparisonDatabase, chain);
        for (Difference difference : differences.getDifferences()) {
            if (difference.getField().equals("tblProperties")) {
                AlterViewPropertiesChangeDatabricks change = getAlterViewPropertiesChangeDatabricks((View) changedObject, control, difference);

                if (changes == null || changes.length == 0) {
                    changes = new Change[] {change};
                } else {
                    changes = Arrays.copyOf(changes, changes.length + 1);
                    changes[changes.length - 1] = change;
                }
            }
        }
        return changes;
    }
}
