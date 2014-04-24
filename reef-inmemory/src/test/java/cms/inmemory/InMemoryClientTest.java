package cms.inmemory;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unit test for simple App.
 */
public class InMemoryClientTest 
    extends TestCase
{
    final Logger LOG = Logger.getLogger(InMemoryClientTest.class.getName());
    final int JOB_TIMEOUT = 1000000; // 10 sec.
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InMemoryClientTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( InMemoryClientTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testInMemoryClient() throws BindException, InjectionException {
        final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
                .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
                .build();
        LauncherStatus status = InMemoryClient.runInMemory(runtimeConf, JOB_TIMEOUT);
        LOG.log(Level.INFO, "InMemory job completed: {0}", status);
        assertTrue(status.isDone());
    }
}
