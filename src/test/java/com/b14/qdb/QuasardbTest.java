/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.b14.qdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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

import com.b14.qdb.batch.Operation;
import com.b14.qdb.batch.Result;
import com.b14.qdb.batch.Results;
import com.b14.qdb.batch.TypeOperation;
import com.b14.qdb.data.Pojo;
import com.b14.qdb.entities.QuasardbEntry;

public class QuasardbTest {
    public static final String HOST = "127.0.0.1";
    public static final String PORT = "2836";
    private static final Map<String,String> config = new HashMap<String,String>();

    private Quasardb qdbInstance = null;

    public QuasardbTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        config.put("name", "test");
        config.put("host", HOST);
        config.put("port", PORT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of close method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testClose() throws Exception {
        // Test : close qdb session
        qdbInstance.close();

        try {
            qdbInstance.put("test_close", "test_close");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // re-initialize qdb session
        setUpClass();
    }

    /**
     * Test of put method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testPut() throws QuasardbException {
        // Test 1.1 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.put("test_put_1", test);
        String result = qdbInstance.get("test_put_1");
        assertTrue(test.equals(result));

        // Test 1.2 : nominal case - pojo
        Pojo pojo = new Pojo();
        qdbInstance.put("test_put_2", pojo);
        Pojo pojoresult = qdbInstance.get("test_put_2");
        assertTrue(pojo.getText().equals(pojoresult.getText()));

        // Test 2.1 : wrong parameter
        try {
            qdbInstance.put("test_put_3", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.2 : wrong parameter
        try {
            qdbInstance.put(null, pojo);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.3 : wrong parameter
        try {
            qdbInstance.put(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.4 : wrong parameter -> alias too long
        String veryLongAlias = "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj";
        try {
            qdbInstance.put(veryLongAlias, "test long alias");
            assertTrue(qdbInstance.get(veryLongAlias).equals("test long alias"));
        } catch (Exception e) {
            fail("Very long alias are allowed now.");
        }

        // Test 3 : put with a key already mapped
        try {
            qdbInstance.put("test_put_2", "test_put_2 key is already mapped");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup Qdb
        qdbInstance.remove("test_put_1");
        qdbInstance.remove("test_put_2");
        qdbInstance.remove(veryLongAlias);
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testGet() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.get(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.get("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.get("test_nominal");
        assertTrue(pojo.getText().equals(pojoresult.getText()));

        // Test 3 : wrong alias
        try {
            qdbInstance.get("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplace() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.getAndReplace(null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getAndReplace("test1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getAndReplace("", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getAndReplace("test_nominal", pojo2);
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo2.getText().equals(pojoGet.getText()));

        // Test 3 : wrong alias
        try {
            qdbInstance.getAndReplace("alias_doesnt_exist", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testGetRemove() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.getRemove(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getRemove("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getRemove("test_nominal");
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        try {
            qdbInstance.get("test_nominal");
            fail("Entry must not exist anymore");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 3 : wrong alias
        try {
            qdbInstance.getRemove("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwap() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.compareAndSwap(null, "test", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("test1", null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("test", "test", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.4 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("", "test", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.1 : nominal case -> swap case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo);
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo2.getText().equals(pojoGet.getText()));
        assertFalse(pojo.getText().equals(pojoGet.getText()));
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;

        // Test 2.2 : nominal case -> no swap case
        qdbInstance.put("test_nominal", pojo);
        Pojo pojo3 = new Pojo();
        pojo3.setText("test3");
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo3);
        assertFalse(pojo3.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo.getText().equals(pojoGet.getText()));

        // Test 3 : wrong alias
        try {
            qdbInstance.compareAndSwap("alias_doesnt_exist", "test","test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of delete method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testRemove() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.remove(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.remove("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.remove("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.put("test_del_1", test);
        qdbInstance.remove("test_del_1");
        try {
            qdbInstance.get("test_del_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }

    /**
     * Test of delete method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testRemoveAll() throws QuasardbException {
        // Test : nominal case - add 4 objects and remove them all
        String test = "Voici un super test";
        qdbInstance.put("test_del_1", test);
        qdbInstance.put("test_del_2", test);
        qdbInstance.put("test_del_3", test);
        qdbInstance.put("test_del_4", test);
        qdbInstance.removeAll();
        try {
            qdbInstance.get("test_del_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_2");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_3");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_4");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }

    /**
     * Test of removeIf method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIf() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.removeIf(null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.removeIf("test1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.removeIf(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.1 : nominal case -> remove case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test1");
        qdbInstance.put("test_nominal", pojo);
        qdbInstance.removeIf("test_nominal", pojo2);
        try {
            qdbInstance.get("test_nominal");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.2 : nominal case -> do not remove case
        pojo = null;
        pojo2 = null;
        pojo = new Pojo();
        pojo.setText("test1");
        pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        try {
            qdbInstance.removeIf("test_nominal", pojo2);
            fail("No exception here.");
        } catch (Exception e) {
            Pojo pojoResult = qdbInstance.get("test_nominal");
            assertTrue(pojoResult.getText().equals(pojo.getText()));
        }

        // Test 3 : wrong alias
        try {
            qdbInstance.removeIf("alias_doesnt_exist", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of update method, of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testUpdate() throws QuasardbException {
        // Test 2.1 : wrong parameter
        try {
            qdbInstance.update("test_update_1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.2 : wrong parameter
        try {
            qdbInstance.update(null, "test_update_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.3 : wrong parameter
        try {
            qdbInstance.update(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2.1 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.update("test_update_1", test);
        String result = qdbInstance.get("test_update_1");
        assertTrue(test.equals(result));

        // Test 2.2 : nominal case - pojo
        Pojo pojo = new Pojo();
        qdbInstance.update("test_update_2", pojo);
        Pojo pojoresult = qdbInstance.get("test_update_2");
        assertTrue(pojo.getText().equals(pojoresult.getText()));

        // Test 3 : update wrong key
        qdbInstance.update("test_update_3", "wrong key");
        String resultat = qdbInstance.get("test_update_3");
        assertTrue(resultat.equals("wrong key"));

        // Cleanup Qdb
        qdbInstance.remove("test_update_1");
        qdbInstance.remove("test_update_2");
        qdbInstance.remove("test_update_3");
    }

    /**
     * Test some errors of class Quasardb.
     * @throws QuasardbException
     */
    @Test
    public void testErrors() throws QuasardbException {
        // Testing timeout
        Map<String,String> config = new HashMap<String,String>();
        config.put("name", "testerror");
        config.put("host", "unknown_host");
        config.put("port", PORT);
        try {
            new Quasardb(config);
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
            assertTrue(e.getMessage().equalsIgnoreCase("Host provided was not found."));
        }

        // Testing empty error
        QuasardbException exception = new QuasardbException();
        assertNull("Exception must be null", exception.getMessage());
    }

   @Test
   public void testGetCurrentNodeConfig() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            Pattern pattern = null;

            String test = qdbInstance.getCurrentNodeConfig();

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
                        fail("No exception allowed here.");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testGetNodeConfig() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            Pattern pattern = null;

            String test = qdbInstance.getNodeConfig("127.0.0.1", 2836);

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
                        fail("No exception allowed here.");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testGetCurrentNodeStatus() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            final String PORT_PATTERN = "\\d{2,6}";
            final String TIMESTAMP_PATTERN = "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}";
            Pattern pattern = null;

            String test = qdbInstance.getCurrentNodeStatus();

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
                        fail("No exception allowed here.");
                    }
                } else if ("listening_port".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(PORT_PATTERN);
                    jp.nextToken(); // listening_port value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("startup".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // startup value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("timestamp".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // timestamp value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testGetNodeStatus() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final String NODE_PATTERN = "\\w{1,16}.\\w{1,16}.\\w{1,16}.\\w{1,16}";
            final String PORT_PATTERN = "\\d{2,6}";
            final String TIMESTAMP_PATTERN = "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}";
            Pattern pattern = null;

            String test = qdbInstance.getNodeStatus("127.0.0.1", 2836);

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
                        fail("No exception allowed here.");
                    }
                } else if ("listening_port".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(PORT_PATTERN);
                    jp.nextToken(); // listening_port value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("node_id".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(NODE_PATTERN);
                    jp.nextToken(); // node_id value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("startup".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // startup value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("timestamp".equals(fieldname) && !"}".equals(jp.getText())) {
                    pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    jp.nextToken(); // timestamp value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testGetCurrentNodeTopology() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

            String test = qdbInstance.getCurrentNodeTopology();

            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(test);
            jp.nextToken();
            while (jp.nextToken() != null) {
                String fieldname = jp.getCurrentName();
                if ("center".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("predecessor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("successor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testGetNodeTopology() throws QuasardbException {
        try {
            final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            final Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

            String test = qdbInstance.getNodeTopology("127.0.0.1", 2836);

            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(test);
            jp.nextToken();
            while (jp.nextToken() != null) {
                String fieldname = jp.getCurrentName();
                if ("center".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("predecessor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                } else if ("successor".equals(fieldname) && !"}".equals(jp.getText())) {
                    jp.nextToken(); // {
                    jp.nextToken(); // endpoint
                    assertTrue(jp.getText().equals("endpoint"));
                    jp.nextToken(); // endpoint value
                    Matcher matcher = pattern.matcher(jp.getText());
                    if (!matcher.find()) {
                        fail("No exception allowed here.");
                    }
                }
            }
        } catch (JsonParseException e) {
            assertTrue(e.getMessage().indexOf("only regular white space") > -1);
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }

    @Test
    public void testIterator() throws QuasardbException {
        // Test 1 : iterate on 100 entries
        int nbIterations = 100;
        try {
            for (int i = 0; i < nbIterations; i++) {
                Pojo p = new Pojo();
                p.setText("test iterator " + i);
                qdbInstance.put("test_iterator_" + i, p);
            }
            int i = 0;
            for (QuasardbEntry<?> qdbe : qdbInstance) {
                assertTrue(qdbInstance.get(qdbe.getAlias()).equals(qdbe.getValue()));
                i++;
            }
            assertTrue(i == (nbIterations - 1));
        } catch (Exception e) {
            fail("No exception allowed.");
        }

        // Test 2 : iterate with no entries
        qdbInstance.removeAll();
        assertFalse(qdbInstance.iterator().hasNext());

        // Test 2 : iterate on 1000 entries
        nbIterations = 1000;
        try {
            for (int i = 0; i < nbIterations; i++) {
                Pojo p = new Pojo();
                p.setAbc(i);
                p.setText("test iterator " + i);
                qdbInstance.put("test_iterator_" + i, p);
            }
            int i = 0;
            for (QuasardbEntry<?> qdbe : qdbInstance) {
                assertTrue(qdbe.getAlias() != null);
                assertTrue(qdbe.getValue() != null);
                assertTrue(qdbe.getValue() instanceof Pojo);
                i++;
            }
            assertTrue(i == (nbIterations - 1));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
        qdbInstance.removeAll();
    }

    @Test
    public void testGetDefaultExpiryTimeInSeconds() throws QuasardbException {
        assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    }

    @Test
    public void testSetDefaultExpiryTimeInSeconds() throws QuasardbException {
        long expiryTime=20L;

        // Test 1 : set default expiry time to 20s
        qdbInstance.setDefaultExpiryTimeInSeconds(expiryTime);
        assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);

        // Test 2 : set default expiry time to eternal
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);

        // Test 3 : negativ value
        try {
            qdbInstance.setDefaultExpiryTimeInSeconds(-1);
            fail("No exception allowed here");
        } catch (QuasardbException e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : test default expiry time with config
        Map<String,String> config2 = new HashMap<String,String>();
        config2.put("name", "test");
        config2.put("host", HOST);
        config2.put("port", PORT);
        config2.put("expiry", "20");
        qdbInstance = new Quasardb(config2);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);

        // Clean up
        config2 = null;
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    }

    @Test
    public void testPutWithExpiryTime() throws QuasardbException {
        long expiry = 1L;

        // Test 1 : nominal case
        String test = "Voici un super test";
        qdbInstance.put("test_put_expiry_1", test, expiry);
        String result = qdbInstance.get("test_put_expiry_1");
        assertTrue(test.equals(result));
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_put_expiry_1");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
/*
        // Test 2 : no expiry
        qdbInstance.put("test_put_expiry_1", test, -1);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue(((String) qdbInstance.get("test_put_expiry_1")).equalsIgnoreCase(test));

        // Test 3 : expiry time = current time + 2 seconds
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        long calculatedExpiry = cal.getTimeInMillis()/1000;
        qdbInstance.put("test_put_expiry_2", test, 2L);
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") == calculatedExpiry);
        try {
            Thread.sleep(3L * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }

        try {
            qdbInstance.get("test_put_expiry_2");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : put entries with different expiry times
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.put("test_put_expiry_2", test);
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") == 0);
        qdbInstance.put("test_put_expiry_3", test, expiry);
        qdbInstance.put("test_put_expiry_4", test, expiry + expiry);
        try {
            Thread.sleep((expiry + (expiry / 2)) * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue(((String) qdbInstance.get("test_put_expiry_2")).equalsIgnoreCase(test));
        try {
            qdbInstance.get("test_put_expiry_3");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        assertTrue(((String) qdbInstance.get("test_put_expiry_4")).equalsIgnoreCase(test));
        try {
            Thread.sleep(expiry * 5000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_put_expiry_4");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup Qdb
        qdbInstance.remove("test_put_expiry_1");
        qdbInstance.remove("test_put_expiry_2");*/
    }

    @Test
    public void testSetExpiryTimeInSeconds() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.removeAll();

        // Test 1 : nominal case
        String test = "Voici un super test";
        qdbInstance.put("test_expiry_1", test);
        try {
            Thread.sleep(1L * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue(((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", 1L);
        try {
            Thread.sleep(1L * 1000 + 500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_expiry_1");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        long calculatedExpiry = cal.getTimeInMillis()/1000;
        qdbInstance.put("test_expiry_1", test, 2L);
        try {
            Thread.sleep(1L * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue(((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        assertEquals(qdbInstance.getExpiryTimeInSeconds("test_expiry_1"), calculatedExpiry, 1);

        // Test 2 : negativ param
        try {
            qdbInstance.setExpiryTimeInSeconds("test_expiry_1", -1);
            fail("No exception allowed.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 3 : invalid alias
        try {
            qdbInstance.setExpiryTimeInSeconds("wrong_alias", 1L);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : null alias
        try {
            qdbInstance.setExpiryTimeInSeconds(null, 1L);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup Qdb
        qdbInstance.remove("test_expiry_1");
    }

    @Test
    public void testGetExpiryTimeInSeconds() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.removeAll();
        String test = "Voici un super test";

        // Test 1.1 : nominal case
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
         cal.setTimeZone(TimeZone.getTimeZone("UTC"));
         long calculatedExpiry = cal.getTimeInMillis()/1000;
        qdbInstance.put("test_expiry_1", test, 2L);
        try {
             Thread.sleep(1L * 1000);
         } catch (InterruptedException e1) {
             fail("No exception allowed.");
         }
        assertEquals(qdbInstance.getExpiryTimeInSeconds("test_expiry_1"), calculatedExpiry, 1);

        // Test 1.2 : eternal expiry time
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", 0);
        try {
             Thread.sleep(1L * 1000 + 500);
        } catch (InterruptedException e1) {
             fail("No exception allowed.");
        }
        assertEquals(qdbInstance.getExpiryTimeInSeconds("test_expiry_1"), 0);

        // Test 2 : invalid alias
        try {
            qdbInstance.getExpiryTimeInSeconds("wrong_alias");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 3 : null alias
        try {
            qdbInstance.getExpiryTimeInSeconds(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : empty alias
        try {
            qdbInstance.getExpiryTimeInSeconds("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Cleanup Qdb
        qdbInstance.remove("test_expiry_1");
    }

    @Test
    public void testSetExpiryTimeAt() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.removeAll();
        String test = "Voici un super test";
        qdbInstance.put("test_expiry_1", test);
        long time = System.currentTimeMillis() + (1000 * 60 * 60);
        Date expiryDate = new Date(time);

        // Test 1 : null param
        try {
            qdbInstance.setExpiryTimeAt(null, expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : null date
        try {
            qdbInstance.setExpiryTimeAt("test_expiry_1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 3 : empty alias
        try {
            qdbInstance.setExpiryTimeAt("", expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : wrong alias
        try {
            qdbInstance.setExpiryTimeAt("wrong_alias", expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 5 : nominal case
        qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
        String sTime = "" + time;
        assertTrue((qdbInstance.getExpiryTimeInSeconds("test_expiry_1") + "").equals(sTime.substring(0, sTime.length() - 3)));

        // Cleanup Qdb
        qdbInstance.removeAll();
    }

    @Test
    public void testGetExpiryTimeInDate() throws QuasardbException {
        qdbInstance.removeAll();

        // Test 1 : null param
        try {
            qdbInstance.getExpiryTimeInDate(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : empty alias
        try {
            qdbInstance.getExpiryTimeInDate("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : wrong alias
        try {
            qdbInstance.getExpiryTimeInDate("wrong_alias");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 4 : nominal case
        long time = System.currentTimeMillis() + (1000 * 60 * 60);
        Date expiryDate = new Date(time);
        qdbInstance.put("test_expiry_1", new String("test_expiry_1"));
        qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
        assertTrue(qdbInstance.getExpiryTimeInDate("test_expiry_1").toString().equals(expiryDate.toString()));

        // Cleanup Qdb
        qdbInstance.removeAll();
    }

    @Test
    public void testStartsWith() throws QuasardbException {
        qdbInstance.removeAll();

        // Test 1 : null param
        try {
            qdbInstance.startsWith(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : empty alias
        try {
            qdbInstance.startsWith("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 3 : no matches
        for (int i = 0; i < 10; i++) {
            Pojo value = new Pojo();
            value.setText("test number " + i);
            qdbInstance.put("prefix1.startWith_" + i, value);
            if (i % 2 == 0) {
                qdbInstance.put("prefix2.startWith_" + i, value);
            }
        }
        List<String> result = null;
        try {
            result = qdbInstance.startsWith("unknow_prefix");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        assertTrue(result == null);

        // Test 4 : nominal case
        result = qdbInstance.startsWith("prefix1");
        assertTrue(result.size() == 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(result.get(i).equalsIgnoreCase("prefix1.startWith_" + i));
            assertTrue(((Pojo) qdbInstance.get(result.get(i))).getText().equalsIgnoreCase("test number " + i));
        }
        result = null;
        result = qdbInstance.startsWith("prefix2");
        assertTrue(result.size() == 5);
        for (int i = 0; i < 5; i++) {
            assertTrue(result.get(i).equalsIgnoreCase("prefix2.startWith_" + (i*2)));
            assertTrue(((Pojo) qdbInstance.get(result.get(i))).getText().equalsIgnoreCase("test number " + (i*2)));
        }
        result = null;

        // Cleanup Qdb
        qdbInstance.removeAll();
    }

    @Test
    public void testRunBatch() throws QuasardbException {
        qdbInstance.removeAll();

        // Test 1 : null param
        try {
            qdbInstance.runBatch(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }

        // Test 2 : nominal cases - valid batch operations
        //  -> Test 2.1 : nominal case -> GET
        Pojo testGet = new Pojo();
        testGet.setText("test_batch_get");
        qdbInstance.put("test_batch_get", testGet);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        Operation<Pojo> operationGet = new Operation<Pojo>(TypeOperation.GET, "test_batch_get");
        operations.add(operationGet);
        Results results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET);
            assertTrue(result.getError() == null);
            assertTrue(((Pojo) result.getValue()).getText().equals("test_batch_get"));
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.2 : nominal case -> PUT
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_put");
        Operation<Pojo> operationPut = new Operation<Pojo>(TypeOperation.PUT, "test_batch_put", testPut);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationPut);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_put"));
            assertTrue(result.getTypeOperation() == TypeOperation.PUT);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() == null);
            Pojo test = qdbInstance.get("test_batch_put");
            assertTrue(test.getText().equals("test_batch_put"));
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.3 : nominal case -> UPDATE
        qdbInstance.put("test_batch_update", "test");
        assertTrue(((String) qdbInstance.get("test_batch_update")).equals("test"));
        Pojo testUpdate = new Pojo();
        testUpdate.setText("test_batch_update");
        Operation<Pojo> operationUpdate = new Operation<Pojo>(TypeOperation.UPDATE, "test_batch_update", testUpdate);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationUpdate);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_update"));
            assertTrue(result.getTypeOperation() == TypeOperation.UPDATE);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() == null);
            Pojo test = qdbInstance.get("test_batch_update");
            assertTrue(test.getText().equals("test_batch_update"));
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.4 : nominal case -> REMOVE
        Pojo testRemove = new Pojo();
        testRemove.setText("test_batch_remove");
        qdbInstance.put("test_batch_remove", testRemove);
        Operation<Pojo> operationRemove = new Operation<Pojo>(TypeOperation.REMOVE, "test_batch_remove");
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemove);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_remove"));
            assertTrue(result.getTypeOperation() == TypeOperation.REMOVE);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() == null);
            try {
                qdbInstance.get("test_batch_remove");
                fail("An exception must be thrown.");
            } catch (Exception e) {
                assertTrue(e instanceof QuasardbException);
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.5 : nominal case -> CAS
        Pojo testCas1 = new Pojo();
        testCas1.setText("test_batch_cas");
        qdbInstance.put("test_batch_cas", testCas1);
        assertTrue(((Pojo) qdbInstance.get("test_batch_cas")).getText().equals("test_batch_cas"));
        Pojo testCas2 = new Pojo();
        testCas2.setText("test_batch_cas_2");

        Operation<Pojo> operationCas = new Operation<Pojo>(TypeOperation.CAS, "test_batch_cas", testCas1, testCas2);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationCas);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_cas"));
            assertTrue(result.getTypeOperation() == TypeOperation.CAS);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() != null);
            assertTrue(((Pojo) result.getValue()).getText().equals("test_batch_cas")); // always return old value
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.6 : nominal case -> GET REMOVE
        Pojo testGetRemove = new Pojo();
        testGetRemove.setText("test_batch_get_remove");
        qdbInstance.put("test_batch_get_remove", testGetRemove);
        assertTrue(((Pojo) qdbInstance.get("test_batch_get_remove")).getText().equals("test_batch_get_remove"));

        Operation<Pojo> operationGetRemove = new Operation<Pojo>(TypeOperation.GET_REMOVE, "test_batch_get_remove");
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetRemove);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get_remove"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET_REMOVE);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() != null);
            assertTrue(((Pojo) result.getValue()).getText().equals("test_batch_get_remove"));
        }
        try {
            qdbInstance.get("test_batch_get_remove");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.7 : nominal case -> GET UPDATE
        Pojo testGetUpdate = new Pojo();
        testGetUpdate.setText("test_batch_get_update");
        qdbInstance.put("test_batch_get_update", testGetUpdate);
        assertTrue(((Pojo) qdbInstance.get("test_batch_get_update")).getText().equals("test_batch_get_update"));
        Pojo testGetUpdate2 = new Pojo();
        testGetUpdate2.setText("test_batch_get_update_2");

        Operation<Pojo> operationGetUpdate = new Operation<Pojo>(TypeOperation.GET_UPDATE, "test_batch_get_update", testGetUpdate2);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetUpdate);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get_update"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET_UPDATE);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() != null);
            assertTrue(((Pojo) result.getValue()).getText().equals("test_batch_get_update"));
            assertTrue(((Pojo) qdbInstance.get("test_batch_get_update")).getText().equals("test_batch_get_update_2"));
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 2.8 : nominal case -> REMOVE IF
        Pojo testRemoveIf = new Pojo();
        testRemoveIf.setText("test_batch_remove_if");
        qdbInstance.put("test_batch_remove_if", testRemoveIf);
        Operation<Pojo> operationRemoveIf = new Operation<Pojo>(TypeOperation.REMOVE_IF, "test_batch_remove_if", testRemoveIf);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemoveIf);
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_remove_if"));
            assertTrue(result.getTypeOperation() == TypeOperation.REMOVE_IF);
            assertTrue(result.getError() == null);
            assertTrue(result.getValue() == null);
            try {
                qdbInstance.get("test_batch_remove_if");
                fail("An exception must be thrown.");
            } catch (Exception e) {
                assertTrue(e instanceof QuasardbException);
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        // Test 3 : Failed batch operations
        //  -> Test 3.1 : Fail a GET
        operations = new ArrayList<Operation<Pojo>>();
        operationGet = new Operation<Pojo>(TypeOperation.GET, "test_batch_get");
        operations.add(operationGet);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.2 : Fail a PUT
        operationPut = new Operation<Pojo>(TypeOperation.PUT, "test_batch_put", null);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationPut);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_put"));
            assertTrue(result.getTypeOperation() == TypeOperation.PUT);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
            try {
                qdbInstance.get("test_batch_put");
                fail("An exception must be thrown.");
            } catch (Exception e) {
                assertTrue(e instanceof QuasardbException);
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.3 : Fail a UPDATE
        operationUpdate = new Operation<Pojo>(TypeOperation.UPDATE, "test_batch_update", null);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationUpdate);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_update"));
            assertTrue(result.getTypeOperation() == TypeOperation.UPDATE);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
            try {
                qdbInstance.get("test_batch_update");
                fail("An exception must be thrown.");
            } catch (Exception e) {
                assertTrue(e instanceof QuasardbException);
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.4 : Fail a REMOVE
        operationRemove = new Operation<Pojo>(TypeOperation.REMOVE, "test_batch_remove");
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemove);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_remove"));
            assertTrue(result.getTypeOperation() == TypeOperation.REMOVE);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.5 : Fail a CAS
        qdbInstance.put("test_batch_cas", testCas1);
        assertTrue(((Pojo) qdbInstance.get("test_batch_cas")).getText().equals("test_batch_cas"));
        operationCas = new Operation<Pojo>(TypeOperation.CAS, "test_batch_cas_fail", testCas1, testCas2);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationCas);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_cas_fail"));
            assertTrue(result.getTypeOperation() == TypeOperation.CAS);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.6 : Fail a GET REMOVE
        operationGetRemove = new Operation<Pojo>(TypeOperation.GET_REMOVE, "test_batch_get_remove");
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetRemove);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get_remove"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET_REMOVE);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.7 : Fail a GET UPDATE
        operationGetUpdate = new Operation<Pojo>(TypeOperation.GET_UPDATE, "test_batch_get_update", testGetUpdate2);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetUpdate);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_get_update"));
            assertTrue(result.getTypeOperation() == TypeOperation.GET_UPDATE);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        //  -> Test 3.8 : Fail a REMOVE IF
        operationRemoveIf = new Operation<Pojo>(TypeOperation.REMOVE_IF, "test_batch_remove_if", testRemoveIf);
        operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemoveIf);
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_remove_if"));
            assertTrue(result.getTypeOperation() == TypeOperation.REMOVE_IF);
            assertTrue(result.getError() != null);
            assertTrue(result.getValue() == null);
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        // Test 4 : simple successful put operations
        testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            operations.add(operation);
        }
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        int it = 0;
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().equals("test_batch_simple_" + it));
            assertTrue(result.getError() == null);
            assertTrue(((Pojo) qdbInstance.get(result.getAlias())).getText().equals("test_batch_simple_put"));
            it++;
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        // Test 5 : multiple successful operations
        Pojo test = new Pojo();
        test.setText("test_batch");
        testPut = new Pojo();
        testPut.setText("test_batch_put");
        testGetUpdate = new Pojo();
        testGetUpdate.setText("test_batch_get_update");
        operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            qdbInstance.put( "test_batch_" + i, test);
            // PUT values
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_" + (300 + i), testPut);
            operations.add(operation);
            // GET values
            if (i <= 100) {
                operation = new Operation<Pojo>(TypeOperation.GET, "test_batch_" + i);
                operations.add(operation);
            }
            // GET UPDATE values
            if ((i > 100) && (i <= 200)) {
                operation = new Operation<Pojo>(TypeOperation.GET_UPDATE, "test_batch_" + i, testGetUpdate);
                operations.add(operation);
            }
            // REMOVE
            if (i > 200) {
                operation = new Operation<Pojo>(TypeOperation.REMOVE, "test_batch_" + i);
                operations.add(operation);
            }
        }
        results = qdbInstance.runBatch(operations);
        assertTrue(results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().startsWith("test_batch_"));
            assertTrue(result.getError() == null);
            if (result.getTypeOperation() == TypeOperation.PUT) {
                assertTrue(result.getTypeOperation() == TypeOperation.PUT);
                assertTrue(((Pojo) qdbInstance.get(result.getAlias())).getText().equals("test_batch_put"));
            } else if (result.getTypeOperation() == TypeOperation.GET) {
                assertTrue(((Pojo) result.getValue()).getText().equals("test_batch"));
            } else if (result.getTypeOperation() == TypeOperation.GET_UPDATE) {
                assertTrue(((Pojo) result.getValue()).getText().equals("test_batch"));
                assertTrue(((Pojo) qdbInstance.get(result.getAlias())).getText().equals("test_batch_get_update"));
            } else if (result.getTypeOperation() == TypeOperation.REMOVE) {
                assertTrue(result.getValue() == null);
                try {
                    qdbInstance.get(result.getAlias());
                    fail("An exception must be thrown.");
                } catch (Exception e) {
                    assertTrue(e instanceof QuasardbException);
                }
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        // Test 6 : simple put operations with entry errors => all operations are in error.
        testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            if (i % 2 != 0) {
                operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_error_" + i, null);
            }
            operations.add(operation);
        }
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().startsWith("test_batch_simple_"));
            assertTrue(result.getError() != null);
            try {
                qdbInstance.get(result.getAlias());
                fail("An exception must be thrown.");
            } catch (Exception e) {
                assertTrue(e instanceof QuasardbException);
            }
        }
        operations = null;
        results = null;
        qdbInstance.removeAll();

        // Test 7 : simple put operations with legal errors
        testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            if (i % 2 != 0) {
                operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + (i - 1), testPut);
            }
            operations.add(operation);
        }
        results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        it = 0;
        for (Result<?> result : results.getResults()) {
            assertTrue(result.getAlias().startsWith("test_batch_simple_"));
            if (result.getError() == null) {
                it++;
            } else {
                assertTrue(((Pojo) qdbInstance.get(result.getAlias())).getText().equals("test_batch_simple_put"));
                it--;
            }
        }
        assertTrue(it == 0);
        operations = null;
        results = null;
        qdbInstance.removeAll();

    }
}
