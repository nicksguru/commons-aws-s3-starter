package guru.nicks.commons.cucumber;

import guru.nicks.commons.cucumber.world.TextWorld;

import io.cucumber.spring.CucumberContextConfiguration;
import org.mockito.Mock;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Initializes Spring context for the whole test suite. Therefore, initializes beans shared by all scenarios. Mocking
 * should be done inside step definition classes to let them program a different behavior.
 * <p>
 * Please keep in mind that mocked Spring beans ({@link MockitoBean @MockitoBean}) declared in step definition classes
 * conflict with each other because all the steps are part of the same test suite i.e. Spring context. POJO mocks
 * ({@link Mock @Mock}) do not conflict with each other.
 */
@CucumberContextConfiguration
@ContextConfiguration(classes = {
        // scenario-scoped states
        TextWorld.class
})
public class CucumberBootstrap {
}
