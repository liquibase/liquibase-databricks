package liquibase.ext.databricks.change.createTable;

import liquibase.ext.databricks.parser.NamespaceDetailsDatabricks;
import liquibase.serializer.AbstractLiquibaseSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class ExtendedTableProperties extends AbstractLiquibaseSerializable{
    private String tableLocation;
    private String tblProperties;

    @Override
    public String getSerializedObjectName() {
        return "extendedTableProperties";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return NamespaceDetailsDatabricks.DATABRICKS_NAMESPACE;
    }
}
