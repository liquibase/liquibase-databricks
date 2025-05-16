package liquibase.ext.databricks.database

import liquibase.database.DatabaseConnection
import spock.lang.Specification

class DatabricksDatabaseTest extends Specification {

    def isCorrectDatabaseImplementation() {
        when:
        def connection = Mock(DatabaseConnection)
        connection.getDatabaseProductName() >> productName

        then:
        new DatabricksDatabase().isCorrectDatabaseImplementation(connection) == expected

        where:
        productName  | expected
        "Databricks" | true
        "databricks" | true
        "Sparksql"   | true
        "SparkSQL"   | true
        "oracle"     | false
    }

    def shouldHandleStringEscapingCorrectly() {
        when:
        def database = new DatabricksDatabase()
        def result = database.escapeStringForDatabase(input)

        then:
        result == output

        where:
        input                       | output
        "a message"                 | "a message"
        "the 'quoted'"              | "the \\'quoted\\'"
        "mixed \\'quoted'"          | "mixed \\'quoted\\'"
        "mixed \\\"quotes'"         | "mixed \\\"quotes\\'"
        "mixed \\\"quotes\\''\\'"   | "mixed \\\"quotes\\'\\'\\'"
    }
}
