package cms.inmemory;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class InMemoryDriverTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InMemoryDriverTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( InMemoryDriverTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testInMemoryClient()
    {
        assertTrue( true );
    }
}
