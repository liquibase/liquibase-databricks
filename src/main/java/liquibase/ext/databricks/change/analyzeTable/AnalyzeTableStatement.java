package liquibase.ext.databricks.change.analyzeTable;

import liquibase.statement.AbstractSqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;

@Getter
@Setter
public class AnalyzeTableStatement extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private List<String> analyzeColumns = new ArrayList<>();
    private Map<String, String> partition = Collections.emptyMap();

}
