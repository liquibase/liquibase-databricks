package liquibase.ext.databricks.change.alterCluster;

import liquibase.serializer.AbstractLiquibaseSerializable;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
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

}
