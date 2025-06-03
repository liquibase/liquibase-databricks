package liquibase.ext.databricks.database

import liquibase.database.jvm.JdbcConnection
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.SQLException

class DatabricksConnectionPatternsTest extends Specification {

    @Unroll
    def "sanitizeUrl"() {
        when:
        def passedInput = input
        def conn = new JdbcConnection() {
            @Override
            protected String getConnectionUrl() throws SQLException {
                return passedInput
            }
        }

        then:
        JdbcConnection.sanitizeUrl(input) == output

        where:
        input                                                                                                                                                                                      | output
        "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=3;UID=token;PWD=my-personal-access-token"                                                                         | "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=3;UID=token;PWD=*****"
        "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=0;Auth_AccessToken=my-oauth-token"                                                                   | "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=0;Auth_AccessToken=*****"
        "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=2;TokenCachePassPhrase=my-passphrase;EnableTokenCache=0"                                             | "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=2;TokenCachePassPhrase=*****;EnableTokenCache=0"
        "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=1;OAuth2ClientId=my-service-principal-application-id;OAuth2Secret=my-service-principal-oauth-secret" | "jdbc:databricks://my-server-hostname:443;httpPath=my-http-path;AuthMech=11;Auth_Flow=1;OAuth2ClientId=my-service-principal-application-id;OAuth2Secret=*****"
        null                                                                                                                                                                                       | null
    }
}
