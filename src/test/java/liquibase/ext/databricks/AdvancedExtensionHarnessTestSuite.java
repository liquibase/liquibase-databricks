package liquibase.ext.databricks;

import liquibase.harness.AdvancedHarnessSuite;
import liquibase.harness.snapshot.SnapshotObjectTests;
import org.junit.platform.suite.api.SelectClasses;

@SelectClasses({SnapshotObjectTests.class})
public class AdvancedExtensionHarnessTestSuite extends AdvancedHarnessSuite {
}
