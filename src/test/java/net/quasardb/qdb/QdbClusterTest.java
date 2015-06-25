package net.quasardb.qdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Matchers.any;

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
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdbJNI;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link QdbCluster} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version 2.0.0
 * @since 1.1.6
 */

public class QdbClusterTest {

    private static final String URI = "qdb://127.0.0.1:2836";
    private QdbCluster cluster = null;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Qdb.DAEMON.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Qdb.DAEMON.stop();
    }
    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(URI);
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
     * Test of method {@link QdbCluster#getNodeConfig(String)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testGetNodeConfig() throws QdbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            Pattern pattern = null;

            String test = cluster.getNodeConfig(URI);

            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(test);
            jp.nextToken();
            while (jp.nextToken() != null) {
                String fieldname = jp.getCurrentName();
                if ("listen_on".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(IPADDRESS_PATTERN);
                    jp.nextToken(); // listen_on value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON listen_on value should match IP Address pattern");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON node_id value should match NODE pattern");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue("JsonParseException message should contain 'only regular white space'", e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("The only exception allowed here is JsonParseException.");
        }
    }
    
    /**
     * Test of method {@link QdbCluster#getNodeStatus(String)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testGetNodeStatus() throws QdbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            final String PORT_PATTERN = "\\d{2,6}";
            final String TIMESTAMP_PATTERN = "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}";
            Pattern pattern = null;

            String test = cluster.getNodeStatus(URI);

            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(test);
            jp.nextToken();
            while (jp.nextToken() != null) {
                String fieldname = jp.getCurrentName();
                if ("listening_address".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(IPADDRESS_PATTERN);
                    jp.nextToken(); // listening_address value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON listen_on value should match IP Address pattern");
                    }
                } else if ("listening_port".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(PORT_PATTERN);
                    jp.nextToken(); // listening_port value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON listening_port value should match PORT pattern");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON node_id value should match NODE pattern");
                    }
                } else if ("startup".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // startup value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON startup value should match TIMESTAMP pattern");
                    }
                } else if ("timestamp".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // timestamp value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON timestamp value should match TIMESTAMP pattern");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue("JsonParseException message should contain 'only regular white space'", e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("The only exception allowed here is JsonParseException.");
        }
    }
    
    /**
     * Test of method {@link QdbCluster#getCurrentNodeTopology(String)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testGetNodeTopology() throws QdbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

            String test = cluster.getNodeTopology(URI);

            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(test);
            jp.nextToken();
            while (jp.nextToken() != null) {
                String fieldname = jp.getCurrentName();
                if ("center".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue("Attribute value should be endpoint", jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON center.endpoint value should match IP Address pattern");
                    }
                } else if ("predecessor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue("Attribute value should be endpoint", jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON predecessor.endpoint value should match IP Address pattern");
                    }
                } else if ("successor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue("Attribute value should be endpoint", jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("JSON successor.endpoint value should match IP Address pattern");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue("JsonParseException message should contain 'only regular white space'", e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("The only exception allowed here is JsonParseException.");
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
