package liquibase.ext.databricks.change.addLookupTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.change.core.*;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.change.*;
import liquibase.database.Database;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.core.Column;
import liquibase.change.core.AddLookupTableChange;

import static liquibase.statement.SqlStatement.EMPTY_SQL_STATEMENT;

/**
 * Extracts data from an existing column to create a lookup table.
 * A foreign key is created between the old column and the new lookup table.
 */
@DatabaseChange(name = "addLookupTable", priority = PrioritizedService.PRIORITY_DATABASE, appliesTo = "column",
        description = "Creates a lookup table containing values stored in a column and creates a foreign key to the new table.")
public class AddLookupTableChangeDatabricks extends AddLookupTableChange {

    public String getFinalConstraintName() {
        if (getConstraintName() == null) {
            return ("fk_" + getExistingTableName() + "_" + getNewTableName()).toLowerCase();
        } else {
            return getConstraintName();
        }
    }

    @Override
    public boolean supports(Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        String newTableCatalogName = getNewTableCatalogName();
        String newTableSchemaName = getNewTableSchemaName();

        String existingTableCatalogName = getExistingTableCatalogName();
        String existingTableSchemaName = getExistingTableSchemaName();

        SqlStatement[] createTablesSQL = {new RawParameterizedSqlStatement("CREATE TABLE " + database.escapeTableName(newTableCatalogName, newTableSchemaName
                , getNewTableName())
                + " USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name') "
                + " AS SELECT DISTINCT " + database.escapeObjectName(getExistingColumnName(), Column.class)
                + " AS " + database.escapeObjectName(getNewColumnName(), Column.class)
                + " FROM " + database.escapeTableName(existingTableCatalogName, existingTableSchemaName, getExistingTableName())
                + " WHERE " + database.escapeObjectName(getExistingColumnName(), Column.class)
                + " IS NOT NULL")
        };

        List<SqlStatement> statements = new ArrayList<>(Arrays.asList(createTablesSQL));

        AddNotNullConstraintChange addNotNullChange = new AddNotNullConstraintChange();
        addNotNullChange.setSchemaName(newTableSchemaName);
        addNotNullChange.setTableName(getNewTableName());
        addNotNullChange.setColumnName(getNewColumnName());
        addNotNullChange.setColumnDataType(getNewColumnDataType());
        statements.addAll(Arrays.asList(addNotNullChange.generateStatements(database)));

        AddPrimaryKeyChange addPKChange = new AddPrimaryKeyChange();
        addPKChange.setSchemaName(newTableSchemaName);
        addPKChange.setTableName(getNewTableName());
        addPKChange.setColumnNames(getNewColumnName());
        statements.addAll(Arrays.asList(addPKChange.generateStatements(database)));


        AddForeignKeyConstraintChange addFKChange = new AddForeignKeyConstraintChange();
        addFKChange.setBaseTableSchemaName(existingTableSchemaName);
        addFKChange.setBaseTableName(getExistingTableName());
        addFKChange.setBaseColumnNames(getExistingColumnName());
        addFKChange.setReferencedTableSchemaName(newTableSchemaName);
        addFKChange.setReferencedTableName(getNewTableName());
        addFKChange.setReferencedColumnNames(getNewColumnName());

        addFKChange.setConstraintName(getFinalConstraintName());
        statements.addAll(Arrays.asList(addFKChange.generateStatements(database)));

        return statements.toArray(EMPTY_SQL_STATEMENT);
    }

}