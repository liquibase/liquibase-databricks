package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropForeignKeyConstraintGenerator;
import liquibase.statement.core.DropForeignKeyConstraintStatement;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Table;

public class DropForeignKeyConstraintGeneratorDatabricks extends DropForeignKeyConstraintGenerator {

    @Override
    public boolean supports(DropForeignKeyConstraintStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(DropForeignKeyConstraintStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        // Use lowercase constraint name for Databricks
        String constraintName = statement.getConstraintName().toLowerCase();
        
        return new Sql[] {
            new UnparsedSql("ALTER TABLE " + database.escapeTableName(statement.getBaseTableCatalogName(), statement.getBaseTableSchemaName(), statement.getBaseTableName()) 
                + " DROP CONSTRAINT " + database.escapeConstraintName(constraintName), 
                getAffectedForeignKey(statement))
        };
    }
    
    protected ForeignKey getAffectedForeignKey(DropForeignKeyConstraintStatement statement) {
        return new ForeignKey().setName(statement.getConstraintName()).setForeignKeyTable((Table) new Table().setName(statement.getBaseTableName()).setSchema(statement.getBaseTableCatalogName(), statement.getBaseTableSchemaName()));
    }
}