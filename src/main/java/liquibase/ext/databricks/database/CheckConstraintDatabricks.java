package liquibase.ext.databricks.database;

import liquibase.license.LicenseServiceUtils;
import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.StringUtil;


public class CheckConstraintDatabricks extends AbstractDatabaseObject {
    public CheckConstraintDatabricks() {
    }

    public DatabaseObject[] getContainingObjects() {
        return null;
    }

    public boolean snapshotByDefault() {
        return LicenseServiceUtils.isProLicenseValid();
    }

    public String getName() {
        return (String)this.getAttribute("name", String.class);
    }

    public CheckConstraintDatabricks setName(String var1) {
        this.setAttribute("name", var1);
        return this;
    }

    public Schema getSchema() {
        return this.getTable() == null ? null : this.getTable().getSchema();
    }

    public Table getTable() {
        return (Table)this.getAttribute("table", Table.class);
    }

    public CheckConstraintDatabricks setTable(Table var1) {
        this.setAttribute("table", var1);
        return this;
    }

    public String getBody() {
        return (String)this.getAttribute("body", String.class);
    }

    public CheckConstraintDatabricks setBody(String var1) {
        this.setAttribute("body", var1);
        return this;
    }

    public String toString() {
        Table var1;
        return (var1 = this.getTable()) == null ? this.getName() : this.getName() + " on " + var1.getName();
    }

    public int compareTo(Object var1) {
        return this.equals(var1) ? 0 : super.compareTo(var1);
    }

    public boolean equals(Object var1) {
        if (this == var1) {
            return true;
        } else if (var1 != null && this.getClass() == var1.getClass()) {
            CheckConstraintDatabricks var2 = (CheckConstraintDatabricks)var1;
            return this.getSchema() != null && var2.getSchema() != null ? this.getSchema().toString().equalsIgnoreCase(var2.getSchema().toString()) : this.getName().equalsIgnoreCase(var2.getName());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return StringUtil.trimToEmpty(this.getName()).toLowerCase().hashCode();
    }
}

