package liquibase.ext.databricks.change.alterTableProperties;

import liquibase.serializer.AbstractLiquibaseSerializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SetExtendedTableProperties extends AbstractLiquibaseSerializable {
    private String tblProperties;

    @Override
    public String getSerializedObjectName() {
        return "setExtendedTableProperties";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return "http://www.liquibase.org/xml/ns/databricks";
    }
}
