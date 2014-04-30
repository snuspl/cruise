package org.apache.reef.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Unit test for simple App.
 */
public class LaunchTest 
    extends TestCase
{
    final Logger LOG = Logger.getLogger(LaunchTest.class.getName());
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LaunchTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LaunchTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testInMemoryClient() throws BindException, InjectionException {
        final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
                .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
                .build();
        LauncherStatus status = Launch.runInMemory(runtimeConf);
        LOG.log(Level.INFO, "InMemory job completed: {0}", status);
        assertTrue(status.isDone());
    }
}
