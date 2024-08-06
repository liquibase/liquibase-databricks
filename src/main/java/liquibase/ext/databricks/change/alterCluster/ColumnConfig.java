package liquibase.ext.databricks.change.alterCluster;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class ColumnConfig extends AbstractLiquibaseSerializable {

    private String name;

    @Override
    public String getSerializedObjectName() {
        return "column";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return "http://www.liquibase.org/xml/ns/databricks";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
