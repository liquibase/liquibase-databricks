package liquibase.ext.databricks.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.ColumnConstraint;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.AddColumnStatement;
import liquibase.util.StringUtil;
import java.util.Iterator;
import liquibase.ext.databricks.database.DatabricksDatabase;

public class AddColumnGeneratorDatabricks extends AddColumnGenerator {

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        DatabaseDataType columnType = null;
        if (statement.getColumnType() != null) {
            columnType = DataTypeFactory.getInstance().fromDescription(statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""), database).toDatabaseDataType(database);
        }

        String alterTable = " ADD COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName());
        if (columnType != null) {
            alterTable = alterTable + " " + columnType;
        }

        if (statement.isAutoIncrement() && database.supportsAutoIncrement()) {
            AutoIncrementConstraint autoIncrementConstraint = statement.getAutoIncrementConstraint();
            alterTable = alterTable + " " + database.getAutoIncrementClause(autoIncrementConstraint.getStartWith(), autoIncrementConstraint.getIncrementBy(), autoIncrementConstraint.getGenerationType(), autoIncrementConstraint.getDefaultOnNull());
        }

        alterTable = alterTable + this.getDefaultClause(statement, database);
        if (!statement.isNullable()) {
            Iterator var8 = statement.getConstraints().iterator();

            while(var8.hasNext()) {
                ColumnConstraint constraint = (ColumnConstraint)var8.next();
                if (constraint instanceof NotNullConstraint) {
                    NotNullConstraint notNullConstraint = (NotNullConstraint)constraint;
                    if (StringUtil.isNotEmpty(notNullConstraint.getConstraintName())) {
                        alterTable = alterTable + " CONSTRAINT " + database.escapeConstraintName(notNullConstraint.getConstraintName());
                        break;
                    }
                }
            }

            alterTable = alterTable + " NOT NULL";
            if (database instanceof OracleDatabase) {
                alterTable = alterTable + (!statement.shouldValidateNullable() ? " ENABLE NOVALIDATE " : "");
            }
        } else if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase || database instanceof MySQLDatabase || database instanceof MSSQLDatabase && columnType != null && "timestamp".equalsIgnoreCase(columnType.toString())) {
            alterTable = alterTable + " NULL";
        }

        if (statement.isPrimaryKey()) {
            alterTable = alterTable + " PRIMARY KEY";
            if (database instanceof OracleDatabase) {
                alterTable = alterTable + (!statement.shouldValidatePrimaryKey() ? " ENABLE NOVALIDATE " : "");
            }
        }

        if (database instanceof MySQLDatabase && statement.getRemarks() != null) {
            alterTable = alterTable + " COMMENT '" + database.escapeStringForDatabase(StringUtil.trimToEmpty(statement.getRemarks())) + "' ";
        }

        if (statement.getAddBeforeColumn() != null && !statement.getAddBeforeColumn().isEmpty()) {
            alterTable = alterTable + " BEFORE " + database.escapeColumnName(statement.getSchemaName(), statement.getSchemaName(), statement.getTableName(), statement.getAddBeforeColumn()) + " ";
        }

        if (statement.getAddAfterColumn() != null && !statement.getAddAfterColumn().isEmpty()) {
            alterTable = alterTable + " AFTER " + database.escapeColumnName(statement.getSchemaName(), statement.getSchemaName(), statement.getTableName(), statement.getAddAfterColumn());
        }

        return alterTable;
    }


    private String getDefaultClause(AddColumnStatement statement, Database database) {
        String clause = "";
        Object defaultValue = statement.getDefaultValue();
        if (defaultValue != null) {
            if (database instanceof OracleDatabase && defaultValue.toString().startsWith("GENERATED ALWAYS ")) {
                clause = clause + " " + DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database);
            } else {
                if (database instanceof MSSQLDatabase) {
                    String constraintName = statement.getDefaultValueConstraintName();
                    if (constraintName == null) {
                        constraintName = ((MSSQLDatabase)database).generateDefaultConstraintName(statement.getTableName(), statement.getColumnName());
                    }

                    clause = clause + " CONSTRAINT " + constraintName;
                }

                if (defaultValue instanceof DatabaseFunction) {
                    clause = clause + " DEFAULT " + DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database);
                } else {
                    clause = clause + " DEFAULT " + DataTypeFactory.getInstance().fromDescription(statement.getColumnType(), database).objectToSql(defaultValue, database);
                }
            }
        }

        return clause;
    }
}