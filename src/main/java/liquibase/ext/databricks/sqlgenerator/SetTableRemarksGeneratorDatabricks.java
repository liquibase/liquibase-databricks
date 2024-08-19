package liquibase.ext.databricks.sqlgenerator;

import liquibase.ext.databricks.database.DatabricksDatabase;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;
import liquibase.sqlgenerator.core.SetTableRemarksGenerator;
import org.apache.commons.lang3.StringUtils;

public class SetTableRemarksGeneratorDatabricks extends SetTableRemarksGenerator {

    @Override
    public boolean supports(SetTableRemarksStatement statement, Database database) {
        return (database instanceof DatabricksDatabase);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public ValidationErrors validate(SetTableRemarksStatement setTableRemarksStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", setTableRemarksStatement.getTableName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(SetTableRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;
        String remarksEscaped = database.escapeStringForDatabase(StringUtils.trimToEmpty(statement.getRemarks()));

        sql = "COMMENT ON TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " IS '"
                    + remarksEscaped + "'";

        return new Sql[] { new UnparsedSql(sql, getAffectedTable(statement)) };
    }

    protected Relation getAffectedTable(SetTableRemarksStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}