package cms.inmemory;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class InMemoryTaskTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InMemoryTaskTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( InMemoryTaskTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testInMemoryClient()
    {
        assertTrue( true );
    }
}
