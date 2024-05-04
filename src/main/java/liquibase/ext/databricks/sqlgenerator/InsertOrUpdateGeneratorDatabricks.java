package liquibase.ext.databricks.sqlgenerator;


import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.InsertOrUpdateStatement;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import java.util.Date;

public class InsertOrUpdateGeneratorDatabricks extends InsertOrUpdateGenerator {

    @Override
    public boolean supports(InsertOrUpdateStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    @Override
        protected String getInsertStatement(InsertOrUpdateStatement insertOrUpdateStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();

            for (String columnKey : insertOrUpdateStatement.getColumnValues().keySet()) {
                columns.append(",");
                columns.append(columnKey);
                values.append(",");
                values.append(convertToString(insertOrUpdateStatement.getColumnValue(columnKey), database));
            }
            columns.deleteCharAt(0);
            values.deleteCharAt(0);
            return "INSERT (" + columns + ") VALUES (" + values + ")";
        }

        @Override
        protected String getUpdateStatement(InsertOrUpdateStatement insertOrUpdateStatement, Database database, String whereClause, SqlGeneratorChain sqlGeneratorChain) {
            //We don't need 'whereClause' param here, for Snowflake it's only needed in getRecordCheck() method
            StringBuilder sql = new StringBuilder("UPDATE SET ");

            for (String columnKey : insertOrUpdateStatement.getColumnValues().keySet()) {

                // Databricks does not support updating an identity column, so dont update if the column is part of the key youre merging on
                if ((insertOrUpdateStatement.getAllowColumnUpdate(columnKey)) & (!whereClause.contains(columnKey))) {
                    sql.append("target." + columnKey).append(" = ");
                    sql.append(convertToString(insertOrUpdateStatement.getColumnValue(columnKey), database));
                    sql.append(",");
                }
            }
            int lastComma = sql.lastIndexOf(",");
            if (lastComma > -1) {
                sql.deleteCharAt(lastComma);
            }

            return sql.toString();
        }

        @Override
        protected String getRecordCheck(InsertOrUpdateStatement insertOrUpdateStatement, Database database, String whereClause) {
            return "MERGE INTO " + insertOrUpdateStatement.getTableName() + " AS target USING (SELECT 1) AS source ON target." + whereClause + " WHEN MATCHED THEN ";
        }

        @Override
        protected String getElse(Database database) {
            return " WHEN NOT MATCHED THEN ";
        }

        // Copied from liquibase.sqlgenerator.core.InsertOrUpdateGeneratorHsql
        private String convertToString(Object newValue, Database database) {
            String sqlString;
            if ((newValue == null) || "".equals(newValue.toString()) || "NULL".equalsIgnoreCase(newValue.toString())) {
                sqlString = "NULL";
            } else if ((newValue instanceof String) && !looksLikeFunctionCall(((String) newValue), database)) {
                sqlString = "'" + database.escapeStringForDatabase(newValue.toString()) + "'";
            } else if (newValue instanceof Date) {
                sqlString = database.getDateLiteral(((Date) newValue));
            } else if (newValue instanceof Boolean) {
                if (Boolean.TRUE.equals(newValue)) {
                    sqlString = DataTypeFactory.getInstance().getTrueBooleanValue(database);
                } else {
                    sqlString = DataTypeFactory.getInstance().getFalseBooleanValue(database);
                }
            } else {
                sqlString = newValue.toString();
            }
            return sqlString;
        }

        // Databricks orders its merge statement a bit differently (WHEN MATCHED must come first) and aliases are important
    @Override
    public Sql[] generateSql(InsertOrUpdateStatement insertOrUpdateStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder completeSql = new StringBuilder();
        String whereClause = getWhereClause(insertOrUpdateStatement, database);

        // Core Merge Operation
        completeSql.append(getRecordCheck(insertOrUpdateStatement, database, whereClause));

        String updateStatement = getUpdateStatement(insertOrUpdateStatement, database, whereClause, sqlGeneratorChain);
        completeSql.append(updateStatement);

        // Add insert statement to MERGE if not ONLY an update statement
        if (!insertOrUpdateStatement.getOnlyUpdate()) {

            // Run getElse to add the INSERT when not matched clause
            completeSql.append(getElse(database));

            completeSql.append(getInsertStatement(insertOrUpdateStatement, database, sqlGeneratorChain));
        }

        if (!insertOrUpdateStatement.getOnlyUpdate()) {
            completeSql.append(getPostUpdateStatements(database));
        }

        return new Sql[]{
                new UnparsedSql(completeSql.toString(), "", getAffectedTable(insertOrUpdateStatement))
        };
    }
    }