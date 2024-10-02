package liquibase.ext.databricks.structure.core;

import liquibase.structure.core.View;
import lombok.Data;

@Data
public class ViewDatabricks extends View {

    private String tblProperties;
}
