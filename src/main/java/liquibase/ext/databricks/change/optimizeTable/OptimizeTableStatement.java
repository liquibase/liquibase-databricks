package liquibase.ext.databricks.change.optimizeTable;


import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Setter
@Getter
public class OptimizeTableStatement extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private List<String> zorderColumns = new ArrayList<>();

    public void setZorderColumns(ArrayList<String> zorderColumns) {
        this.zorderColumns = zorderColumns;
    }

    public void setZorderColumns(String zorderColumns) {
        if (zorderColumns == null) {
            this.zorderColumns = new ArrayList<>();
            return;
        }
        this.zorderColumns = new ArrayList<>(Arrays.asList(zorderColumns.split("\\s*,\\s*")));
    }

}
