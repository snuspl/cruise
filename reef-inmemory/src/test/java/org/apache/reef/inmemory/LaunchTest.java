package org.apache.reef.inmemory;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class LaunchTest 
    extends TestCase
{
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
    public void testInMemoryClient()
    {
        assertTrue( true );
    }
}
