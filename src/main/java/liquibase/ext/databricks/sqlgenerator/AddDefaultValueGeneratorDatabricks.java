package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.AddDefaultValueStatement;
import liquibase.sqlgenerator.core.AddDefaultValueGenerator;

import java.util.Arrays;
import java.util.List;

public class AddDefaultValueGeneratorDatabricks extends AddDefaultValueGenerator {
    private static final List<String> NUMERIC_TYPES = Arrays.asList("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL");
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddDefaultValueStatement statement, Database database) {
        return database instanceof DatabricksDatabase;
    }

    @Override
    public ValidationErrors validate(AddDefaultValueStatement addDefaultValueStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = super.validate(addDefaultValueStatement, database, sqlGeneratorChain);
        try {
            if (addDefaultValueStatement.getDefaultValue() instanceof DatabaseFunction && database.getDatabaseMajorVersion() < 3) {
                errors.addError("This version of Databricks does not support non-literal default values");
            }
        }
        catch (DatabaseException e){
            Scope.getCurrentScope().getLog(getClass()).fine("Can't get default value");
        }
        return errors;
    }
    @Override
    public Sql[] generateSql(AddDefaultValueStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Object defaultValue = statement.getDefaultValue();
        String columnDataType = statement.getColumnDataType();
        String finalDefaultValue;
        if (defaultValue instanceof DatabaseFunction) {
            finalDefaultValue = defaultValue.toString();
        } else {
            if(NUMERIC_TYPES.contains(columnDataType) && defaultValue instanceof String) {
                finalDefaultValue = defaultValue.toString().replace("'", "").trim();
            } else {
                finalDefaultValue =  DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database);
            }
        }
        return new Sql[]{
                new UnparsedSql("ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " ALTER COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()) + " SET DEFAULT " + finalDefaultValue,
                        getAffectedColumn(statement))
        };
    }
}
