package guru.nicks.commons;

import guru.nicks.commons.cucumber.CucumberTestsBase;

/**
 * Runs al Cucumber tests defined in this Maven module.
 * <p>
 * WARNING: this class' name ends with 'Tests' and not with 'IT' - in order to be run with {@code mvn test} (as if it
 * were a unit test) instead of {@code mvn verify}. The reason is that CI pipelines are often configured to collect test
 * code coverage metrics from unit tests only - {@code mvn verify} is not run.
 */
class CucumberTests extends CucumberTestsBase {
}
