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

        StringBuilder alterTable = new StringBuilder(" ADD COLUMN ").append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()));
        if (columnType != null) {
            alterTable.append(" ").append(columnType);
        }

        if (statement.isAutoIncrement() && database.supportsAutoIncrement()) {
            AutoIncrementConstraint autoIncrementConstraint = statement.getAutoIncrementConstraint();
            alterTable.append(" ").append(database.getAutoIncrementClause(autoIncrementConstraint.getStartWith(), autoIncrementConstraint.getIncrementBy(), autoIncrementConstraint.getGenerationType(), autoIncrementConstraint.getDefaultOnNull()));
        }

        alterTable.append(this.getDefaultClause(statement, database));
        if (!statement.isNullable()) {
            Iterator<ColumnConstraint> var8 = statement.getConstraints().iterator();

            while(var8.hasNext()) {
                ColumnConstraint constraint = var8.next();
                if (constraint instanceof NotNullConstraint) {
                    NotNullConstraint notNullConstraint = (NotNullConstraint)constraint;
                    if (StringUtil.isNotEmpty(notNullConstraint.getConstraintName())) {
                        alterTable.append(" CONSTRAINT ").append(database.escapeConstraintName(notNullConstraint.getConstraintName()));
                        break;
                    }
                }
            }

            alterTable.append(" NOT NULL");
            if (database instanceof OracleDatabase) {
                alterTable.append(!statement.shouldValidateNullable() ? " ENABLE NOVALIDATE " : "");
            }
        } else if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase || database instanceof MySQLDatabase || database instanceof MSSQLDatabase && columnType != null && "timestamp".equalsIgnoreCase(columnType.toString())) {
            alterTable.append(" NULL");
        }

        if (statement.isPrimaryKey()) {
            alterTable.append(" PRIMARY KEY");
            if (database instanceof OracleDatabase) {
                alterTable.append(!statement.shouldValidatePrimaryKey() ? " ENABLE NOVALIDATE " : "");
            }
        }

        if (database instanceof MySQLDatabase && statement.getRemarks() != null) {
            alterTable.append(" COMMENT '").append(database.escapeStringForDatabase(StringUtil.trimToEmpty(statement.getRemarks()))).append("' ");
        }

        if (statement.getAddBeforeColumn() != null && !statement.getAddBeforeColumn().isEmpty()) {
            alterTable.append(" BEFORE ").append(database.escapeColumnName(statement.getSchemaName(), statement.getSchemaName(), statement.getTableName(), statement.getAddBeforeColumn())).append(" ");
        }

        if (statement.getAddAfterColumn() != null && !statement.getAddAfterColumn().isEmpty()) {
            alterTable.append(" AFTER ").append(database.escapeColumnName(statement.getSchemaName(), statement.getSchemaName(), statement.getTableName(), statement.getAddAfterColumn()));
        }

        return alterTable.toString();
    }


    private String getDefaultClause(AddColumnStatement statement, Database database) {
        StringBuilder clause = new StringBuilder();
        Object defaultValue = statement.getDefaultValue();
        if (defaultValue != null) {
            if (database instanceof OracleDatabase && defaultValue.toString().startsWith("GENERATED ALWAYS ")) {
                clause.append(" ").append(DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database));
            } else {
                if (database instanceof MSSQLDatabase) {
                    String constraintName = statement.getDefaultValueConstraintName();
                    if (constraintName == null) {
                        constraintName = ((MSSQLDatabase)database).generateDefaultConstraintName(statement.getTableName(), statement.getColumnName());
                    }

                    clause.append(" CONSTRAINT ").append(constraintName);
                }

                if (defaultValue instanceof DatabaseFunction) {
                    clause.append(" DEFAULT ").append(DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database));
                } else {
                    clause.append(" DEFAULT ").append(DataTypeFactory.getInstance().fromDescription(statement.getColumnType(), database).objectToSql(defaultValue, database));
                }
            }
        }

        return clause.toString();
    }
}
