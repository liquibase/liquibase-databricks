package liquibase.ext.databricks.change.createTable;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class ExtendedTableProperties extends AbstractLiquibaseSerializable{
    private String tableLocation;
    private String tblProperties;

    @Override
    public String getSerializedObjectName() {
        return "extendedTableProperties";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return "http://www.liquibase.org/xml/ns/databricks";
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String getTblProperties() {
        return tblProperties;
    }

    public void setTblProperties(String tblProperties) {
        this.tblProperties = tblProperties;
    }
}
