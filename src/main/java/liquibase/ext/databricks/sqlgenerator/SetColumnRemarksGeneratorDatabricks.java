package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Data;
import liquibase.structure.core.Table;
import liquibase.util.ColumnParentType;
import liquibase.util.StringUtil;
import liquibase.sqlgenerator.core.SetColumnRemarksGenerator;

public class SetColumnRemarksGeneratorDatabricks extends SetColumnRemarksGenerator {

    @Override
    public boolean supports(SetColumnRemarksStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return DatabricksDatabase.DATABRICKS_PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(SetColumnRemarksStatement setColumnRemarksStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", setColumnRemarksStatement.getTableName());
        validationErrors.checkRequiredField("columnName", setColumnRemarksStatement.getColumnName());
        validationErrors.checkDisallowedField("catalogName", setColumnRemarksStatement.getCatalogName(), database, MSSQLDatabase.class);
        if (database instanceof MySQLDatabase) {
            validationErrors.checkRequiredField("columnDataType", StringUtil.trimToNull(setColumnRemarksStatement.getColumnDataType()));
        }
        return validationErrors;
    }

    @Override
    public Warnings warn(SetColumnRemarksStatement statementType, Database database, SqlGeneratorChain<SetColumnRemarksStatement> sqlGeneratorChain) {
        final Warnings warnings = super.warn(statementType, database, sqlGeneratorChain);
        if (database instanceof MySQLDatabase) {
            ((MySQLDatabase) database).warnAboutAlterColumn("setColumnRemarks", warnings);
        }

        return warnings;
    }

    @Override
    public Sql[] generateSql(SetColumnRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        String remarksEscaped = database.escapeStringForDatabase(StringUtil.trimToEmpty(statement.getRemarks()));


        return new Sql[]{new UnparsedSql("ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                    + " ALTER COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()) + " COMMENT '"
                    + remarksEscaped + "'", getAffectedColumn(statement))};

    }

    protected Column getAffectedColumn(SetColumnRemarksStatement statement) {
        return new Column().setName(statement.getColumnName()).setRelation(new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName()));
    }
}