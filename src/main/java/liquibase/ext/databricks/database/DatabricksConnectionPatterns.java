package liquibase.ext.databricks.database;

import liquibase.database.jvm.ConnectionPatterns;

import java.util.regex.Pattern;

// From https://docs.databricks.com/aws/en/integrations/jdbc/authentication
public class DatabricksConnectionPatterns extends ConnectionPatterns {

    //jdbc:databricks://<server-hostname>:443;httpPath=<http-path>;AuthMech=3;UID=token;PWD=<personal-access-token>
    private static final String FILTER_PWD = "(?i)(.+?)PWD=([^;&?]+)[;&]*?(.*?)$";
    //jdbc:databricks://<server-hostname>:443;httpPath=<http-path>;AuthMech=11;Auth_Flow=0;Auth_AccessToken=<oauth-token>
    private static final String FILTER_AUTH_ACCESS_TOKEN = "(?i)(.+?)Auth_AccessToken=([^;&?]+)[;&]*?(.*?)$";
    //jdbc:databricks://<server-hostname>:443;httpPath=<http-path>;AuthMech=11;Auth_Flow=2;TokenCachePassPhrase=<passphrase>;EnableTokenCache=0
    private static final String FILTER_TOKEN_CACHE_PASSPHRASE = "(?i)(.+?)TokenCachePassPhrase=([^;&?]+)[;&]*?(.*?)$";
    //jdbc:databricks://<server-hostname>:443;httpPath=<http-path>;AuthMech=11;Auth_Flow=1;OAuth2ClientId=<service-principal-application-id>;OAuth2Secret=<service-principal-oauth-secret>
    private static final String FILTER_OAUTH2_SECRET = "(?i)(.+?)OAuth2Secret=([^;&?]+)[;&]*?(.*?)$";

    public DatabricksConnectionPatterns() {
        String databricksRegex = "(?i)jdbc:databricks(.*)";
        addJdbcObfuscatePatterns(PatternPair.of(Pattern.compile(databricksRegex), Pattern.compile(FILTER_PWD)));
        addJdbcObfuscatePatterns(PatternPair.of(Pattern.compile(databricksRegex), Pattern.compile(FILTER_AUTH_ACCESS_TOKEN)));
        addJdbcObfuscatePatterns(PatternPair.of(Pattern.compile(databricksRegex), Pattern.compile(FILTER_TOKEN_CACHE_PASSPHRASE)));
        addJdbcObfuscatePatterns(PatternPair.of(Pattern.compile(databricksRegex), Pattern.compile(FILTER_OAUTH2_SECRET)));
    }
}
