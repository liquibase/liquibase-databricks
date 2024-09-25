package liquibase.ext.databricks.sqlgenerator

import liquibase.ext.databricks.change.createView.CreateViewStatementDatabricks
import liquibase.ext.databricks.database.DatabricksDatabase
import liquibase.sqlgenerator.SqlGeneratorFactory
import spock.lang.Specification

class CreateViewGeneratorDatabricksTest extends Specification {

    def "creates a view from a sql"() {
        when:
        def selectQuery = "SELECT SYSDATE FROM DUAL"
        def statement = new CreateViewStatementDatabricks("PUBLIC", "schema", "my_view", selectQuery, false)
        def generators = SqlGeneratorFactory.instance.getGenerators(statement, new DatabricksDatabase())

        then:
        generators.size() > 0
        generators[0] instanceof CreateViewGeneratorDatabricks

        when:
        def sql = SqlGeneratorFactory.instance.generateSql(statement, new DatabricksDatabase())

        then:
        sql.length == 1
        sql[0].toString() == "CREATE VIEW PUBLIC.schema.my_view AS " + selectQuery + ";"
    }

    def "creates a view with tblProperties"() {
        when:
        def selectQuery = "SELECT * FROM mytable"
        def tblProperties = "'external.location'='s3://mybucket/mytable','this.is.my.key'=12,'this.is.my.key2'=true"
        def statement = new CreateViewStatementDatabricks("main", "schema", "my_view", selectQuery, false)
        statement.tblProperties = tblProperties
        def sqla = SqlGeneratorFactory.instance.generateSql(statement, new DatabricksDatabase())

        then:
        sqla.length == 1

        when:
        def sql = sqla[0].toString()

        then:
        sql == "CREATE VIEW main.schema.my_view TBLPROPERTIES (" + tblProperties +  ") AS " + selectQuery + ";"
    }

}

