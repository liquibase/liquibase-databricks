package liquibase.ext.databricks.change.alterTableProperties;

import liquibase.serializer.AbstractLiquibaseSerializable;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class UnsetExtendedTableProperties extends AbstractLiquibaseSerializable{
    private String tblProperties;

    @Override
    public String getSerializedObjectName() {
        return "unsetExtendedTableProperties";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return "http://www.liquibase.org/xml/ns/databricks";
    }
}
