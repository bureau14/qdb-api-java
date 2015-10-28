package net.quasardb.qdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URISyntaxException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdbJNI;
import net.quasardb.qdb.jni.qdb_error_t;

/**
 * A unit test case for {@link QdbCluster} class.
 *
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version 2.0.0
 * @since 1.1.6
 */

public class QdbClusterTest {
    private QdbCluster cluster = null;

    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(DaemonRunner.getURI());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            cluster.purgeAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Test of method {@link QdbCluster#QdbCluster(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testWrongURI() {
        try {
            new QdbCluster("wrong_uri");
        } catch (Exception e) {
            assertTrue("Exception should be a URISyntaxException => " + e, e instanceof URISyntaxException);
        }
    }
}
