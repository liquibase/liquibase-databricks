package liquibase.ext.databricks.change.createTable;


import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.DatabaseObject;
import liquibase.util.StringUtil;

public class DatabricksCreateTableGenerator extends CreateTableGenerator {


    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return super.supports(statement, database) && (database instanceof DatabricksDatabase);
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        // New Code

        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);


        String finalsql = sqls[0].toSql();


        //if (statement instanceof DatabricksCreateTableStatement) {

        DatabricksCreateTableStatement thisStatement = (DatabricksCreateTableStatement) statement;


        if ((!StringUtil.isEmpty(thisStatement.getTableFormat()))) {
            finalsql += " USING " + thisStatement.getTableFormat();
        } else {
            finalsql += " USING delta ";
        }

        // Databricks can decide to have tables live in a particular location. If null, Databricks will handle the location automatically in DBFS

        if (!StringUtil.isEmpty(thisStatement.getTableLocation())) {
            finalsql += " LOCATION '" + thisStatement.getTableLocation() + "'";
        }

        //}

        sqls[0] = new UnparsedSql(finalsql, sqls[0].getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));

        return sqls;

    }

}
