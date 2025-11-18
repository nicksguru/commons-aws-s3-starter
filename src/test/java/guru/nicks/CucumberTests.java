package guru.nicks;

import guru.nicks.commons.cucumber.world.CucumberConstants;

import io.cucumber.junit.platform.engine.Constants;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.Suite;

/**
 * Runs al Cucumber tests defined in this Maven module.
 * <p>
 * WARNING: this class' name ends with 'Tests' and not with 'IT' - in order to be run with {@code mvn test} (as if it
 * were a unit test) instead of {@code mvn verify}. The reason is that CI pipelines are often configured to collect test
 * code coverage metrics from unit tests only - {@code mvn verify} is not run.
 */
@Suite
@IncludeEngines(CucumberConstants.ENGINE_NAME)
@ConfigurationParameter(key = Constants.GLUE_PROPERTY_NAME, value = CucumberConstants.GLUE_PROPERTY)
@ConfigurationParameter(key = Constants.FEATURES_PROPERTY_NAME, value = CucumberConstants.FEATURES_CLASSPATH)
@ConfigurationParameter(key = Constants.PLUGIN_PROPERTY_NAME, value = CucumberConstants.PLUGIN_PROPERTY)
@ConfigurationParameter(key = Constants.FILTER_TAGS_PROPERTY_NAME, value = CucumberConstants.FILTER_TAGS_PROPERTY)
class CucumberTests {
}
