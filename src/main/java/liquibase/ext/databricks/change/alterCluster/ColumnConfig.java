package liquibase.ext.databricks.change.alterCluster;

import liquibase.serializer.AbstractLiquibaseSerializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
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
}
