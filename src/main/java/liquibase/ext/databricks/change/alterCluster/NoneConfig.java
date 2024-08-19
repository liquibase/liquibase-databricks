package liquibase.ext.databricks.change.alterCluster;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class NoneConfig extends AbstractLiquibaseSerializable {

    private String none;

    @Override
    public String getSerializedObjectName() {
        return "clusterBy";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return "http://www.liquibase.org/xml/ns/databricks";
    }

    public String getNone() {
        return none;
    }

    public void setNone(String none) {
        this.none = none;
    }
}
