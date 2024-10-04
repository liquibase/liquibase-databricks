package liquibase.ext.databricks.parser;

import liquibase.parser.LiquibaseParser;
import liquibase.parser.NamespaceDetails;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.serializer.LiquibaseSerializer;
import liquibase.serializer.core.xml.XMLChangeLogSerializer;

/**
 * Namespace details for Databricks extension.
 * It is used by Liquibase when generating changelogs - ie during a snapshot.
 */
public class NamespaceDetailsDatabricks implements NamespaceDetails {

    public static final String DATABRICKS_NAMESPACE = "http://www.liquibase.org/xml/ns/databricks";

    public static final String DATABRICKS_XSD = "http://www.liquibase.org/xml/ns/databricks/liquibase-databricks-latest.xsd";

    @Override
    public int getPriority() {
        return PRIORITY_EXTENSION;
    }

    @Override
    public boolean supports(LiquibaseSerializer serializer, String namespace) {
        return namespaceCorrect(namespace) && serializer instanceof XMLChangeLogSerializer;
    }

    @Override
    public boolean supports(LiquibaseParser parser, String namespace) {
        return namespaceCorrect(namespace) && parser instanceof XMLChangeLogSAXParser;
    }

    @Override
    public String getShortName(String namespace) {
        return "databricks";
    }

    @Override
    public String getSchemaUrl(String namespace) {
        return DATABRICKS_XSD;
    }

    @Override
    public String[] getNamespaces() {
        return new String[]{
                DATABRICKS_NAMESPACE
        };
    }

    private boolean namespaceCorrect(String namespace) {
        return namespace.equals(DATABRICKS_NAMESPACE) || namespace.equals(DATABRICKS_XSD);
    }

}
