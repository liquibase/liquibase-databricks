package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.core.CreateIndexGenerator;


import liquibase.change.AddColumnConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.structure.core.Index;
import liquibase.structure.core.Table;

import java.util.Arrays;
import java.util.Iterator;

/*
Class to support Delta Tables with CLUSTER BY KEYS as indexes with Liquid Clustering
Only supported on delta 3.0 + and DBR 13.2+ and DBSQL


 */
//TODO not used - figure out -fix or delete
public class CreateIndexGeneratorDatabricks extends CreateIndexGenerator {

    @Override
    public boolean supports(CreateIndexStatement statement, Database database) {
        return super.supports(statement, database) && (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(CreateIndexStatement createIndexStatement, Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", createIndexStatement.getTableName());
        validationErrors.checkRequiredField("columns", createIndexStatement.getColumns());

        return validationErrors;
    }

    /**
     Databricks does not really support INDEXes as additional data structure separate to the tables
     They are more table properties in which a table will be clustered by using various multi dimensional clustering algorithms

     Delta Tables have 2 types of indexing/clusters

     1. ZORDER - this is the imperative older version that is not really associated with table properites, it is only an action that can be called and changed.
     Indexes of this type will NOT be supported in this change type because they behave differently and would not map well to the createIndex change type

     2. CLUSTER BY - This is a table property of newer Delta Tables (DBR 13.2 +) that more closely follow the index pattern, and thus will be the only type supported in this change type

     */
    @Override
    public Sql[] generateSql(CreateIndexStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain)
    {
        // Since an Index / CLUSTER BY column can be added/changed at any time, this will generate an ALTER TABLE statement to add / change the column

        // There is also a way to add a CLUSTER column in the table DDL itself
        // but that should be an additional parameter to be added in the CreateTableDatabricks change type.

        // Databricks CLUSTER BY keys do not have index names

        StringBuilder buffer = new StringBuilder();

        buffer.append("ALTER TABLE ");

        buffer.append(database.escapeTableName(statement.getTableCatalogName(), statement.getTableSchemaName(), statement.getTableName())).append(" CLUSTER BY ");

        // get columns to cluster by
        Iterator<AddColumnConfig> iterator = Arrays.asList(statement.getColumns()).iterator();

        // if no cluster columns, then cluster by NONE - un-clustered Delta Table
        if (!iterator.hasNext()) {

            buffer.append(" NONE ");
        }
        else {

            buffer.append("(");

            while (iterator.hasNext()) {
                AddColumnConfig column = iterator.next();

                buffer.append(column.getName());

                if (iterator.hasNext()) {
                    buffer.append(", ");
                }
            }
        }
        buffer.append(")");

        return new Sql[] {new UnparsedSql(buffer.toString(), getAffectedIndex(statement))};
    }

}