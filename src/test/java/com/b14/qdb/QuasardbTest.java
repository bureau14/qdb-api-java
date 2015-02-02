/**
 * Copyright (c) 2009-2015, quasardb SAS
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
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
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
import java.util.List;
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

/**
 * A unit test case for {@link Quasardb} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.6
 */
public class QuasardbTest {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 2836;
    private static final QuasardbConfig config = new QuasardbConfig();
    private Quasardb qdbInstance = null;

    public QuasardbTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        QuasardbNode quasardbNode = new QuasardbNode(HOST, PORT);
        config.addNode(quasardbNode);
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
     * Test of method {@link Quasardb#close()}.
     * @throws QuasardbException
     */
    @Test
    public void testCloseMeansNoMoreOperationsAreAllowed() throws Exception {
        // Test : close qdb session
        qdbInstance.close();
        assertTrue("Close shouldn't raise an Exception", true);

        try {
            qdbInstance.put("test_close", "test_close");
            fail("Should raise an Exception because session is closed => no more operations are allowed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }

        // re-initialize qdb session
        setUpClass();
    }

    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutASimpleObject() throws QuasardbException {
        final String key = "test_put_simple_object";
        try {
            String test = "Voici un super test";
            qdbInstance.put(key, test);
            String result = qdbInstance.get(key);
            assertTrue("result should be equals to the value " + test, test.equals(result));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception =>" + e.getMessage());
        } finally {
            qdbInstance.remove(key);
        }
    }
       
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutAPojo() throws QuasardbException {
        final String key = "test_put_pojo";
        try {
            Pojo pojo = new Pojo();
            qdbInstance.put(key, pojo);
            Pojo pojoresult = qdbInstance.get(key);
            assertTrue("result should be equals to the Pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception =>" + e.getMessage());
        } finally {
            qdbInstance.remove(key);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutWithNullValueMeansException() throws QuasardbException {
        try {
            qdbInstance.put("test_put_null_value", null);
            fail("Should raise an Exception because null Value is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutWithNullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.put(null, "Good value");
            fail("Should raise an Exception because null Key is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutWithNoArgsMeanException() throws QuasardbException {
        try {
            qdbInstance.put(null, null);
            fail("Should raise an Exception because no args means aren't allowed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutAVeryLongAlias() throws QuasardbException {
        final String veryLongAlias = "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj";
        try {
            qdbInstance.put(veryLongAlias, "test long alias");
            assertTrue("Very long alias should be stored in quasardb => " + veryLongAlias, qdbInstance.get(veryLongAlias).equals("test long alias"));
        } catch (Exception e) {
            fail("Very long alias are allowed.");
        } finally {
            qdbInstance.remove(veryLongAlias);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutAReservedAliasMeansException() throws QuasardbException {        
        try {
            qdbInstance.put("qdb.test", "alias is reserved");
            fail("Should raise an Exception because qdb is a reserved namespace.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias => " + ((QuasardbException) e).getCode(), ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testPutAKeyTwiceMeansException() throws QuasardbException {
        final String key = "test_put_twice"; 
        try {
            qdbInstance.put(key, "First put");
            assertTrue("First put should be OK", "First put".equals(qdbInstance.get(key)));
            qdbInstance.put(key, "Second put should be KO.");
            fail("Should raise an Exception because put a key twice is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        } finally {
            qdbInstance.remove(key);
        }
    }

    /**
     * Test of method {@link Quasardb#get(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetNoArgMeansException() throws QuasardbException {
        try {
            qdbInstance.get(null);
            fail("Should raise an Exception because null Key is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#get(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.get("");
            fail("Should raise an Exception because key is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
     
    /**
     * Test of method {@link Quasardb#get(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAPojo() throws QuasardbException {
        final String key = "test_nominal";
        try {
            Pojo pojo = new Pojo();
            qdbInstance.put(key, pojo);
            Pojo pojoresult = qdbInstance.get(key);
            assertTrue("Pojo stored at key [" + key + "] should be equals to " + pojo, pojo.getText().equals(pojoresult.getText()));
        } finally {
            qdbInstance.remove(key);
        }
    }
    
    /**
     * Test of method {@link Quasardb#get(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAUnknownAliasMeansException() throws QuasardbException {
        try {
            qdbInstance.get("alias_doesnt_exist");
            fail("Should raise an Exception because alias is unknown.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceANullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace(null, "test");
            fail("Should raise an Exception because null key is forbidden");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryANullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace(null, "test", 0);
            fail("Should raise an Exception because null key is forbidden");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceANullValueMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("test1", null);
            fail("Should raise an Exception because null value are not allowed");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryANullValueMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("test1", null, 0);
            fail("Should raise an Exception because null value are not allowed");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
     
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceAnEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("", "test");
            fail("Should raise an Exception because empty key is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryAnEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("", "test", 0);
            fail("Should raise an Exception because empty key is forbidden.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceAReservedKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("qdb.test", "test");
            fail("Should raise an Exception because qdb is a reserved namespace.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias => " + ((QuasardbException) e).getCode(), ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
        
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryAReservedKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("qdb.test", "test", 0);
            fail("Should raise an Exception because qdb is a reserved namespace.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias => " + ((QuasardbException) e).getCode(), ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }

    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceAPojo() throws QuasardbException {
        qdbInstance.purgeAll();
        
        final String key = "test_nominal";
        try {
            Pojo pojo = new Pojo();
            pojo.setText("test1");
            Pojo pojo2 = new Pojo();
            pojo2.setText("test2");
            qdbInstance.put(key, pojo);
            Pojo pojoresult = qdbInstance.getAndReplace(key, pojo2);
            assertTrue("Value getted should be equals to pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
            Pojo pojoGet = qdbInstance.get(key);
            assertTrue("Value replaced should be equals to pojo2 " + pojo2, pojo2.getText().equals(pojoGet.getText()));
        } finally {
            qdbInstance.remove(key);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceAnUnknownAliasMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("alias_doesnt_exist", "test");
            fail("Should raise an Exception because alias is unknown.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryANegativeExpiryTimeMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("test", "test", -1);
            fail("Should raise an Exception because expiry time must be a positive value.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiry0MeansNoExpiration() throws QuasardbException {
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getAndReplace("test_nominal", pojo2, 0);
        assertTrue("Old value should be equals to initial pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Replaced value should be equals to new pojo " + pojo2, pojo2.getText().equals(pojoGet.getText()));
    }
    
    
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiryAUnknownAliasMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("alias_doesnt_exist", "test", 0);
            fail("Should raise an Exception because the alias <alias_doesnt_exist> is unknown.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
     
    /**
     * Test of method {@link Quasardb#getAndReplace(String, V, long)}.
     * @throws QuasardbException
     */
    @Test
    public void testGetAndReplaceWithExpiry() throws QuasardbException {
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.remove("test_nominal");
        qdbInstance.put("test_nominal", pojo, 0);
        Pojo pojoresult = qdbInstance.getAndReplace("test_nominal", pojo2, 2);
        assertTrue("Old value should be equals to initial pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Replaced value should be equals to new pojo " + pojo2, pojo2.getText().equals(pojoGet.getText()));
        try {
            Thread.sleep(2500);
        } catch (Exception e) {
            fail("No exception allowed here because process is just in sleeping mode.");
        }
        try {
            qdbInstance.get("test_nominal");
            fail("Should raise an Exception because the alias <test_nominal> has been evicted.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getRemove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetRemoveWithNullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getRemove(null);
            fail("Should raise an exception because null key.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getRemove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetRemoveWithEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.getRemove("");
            fail("Should raise an exception because empty key.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
        
    /**
     * Test of method {@link Quasardb#getRemove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetRemoveAPojo() throws QuasardbException {
        Pojo pojo = new Pojo();
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getRemove("test_nominal");
        assertTrue("Pojo " + pojoresult + " should be equals to Pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
        try {
            qdbInstance.get("test_nominal");
            fail("Should raise an exception because entry should have been removed");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getRemove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetRemoveAWrongAlias() throws QuasardbException {
        try {
            qdbInstance.getRemove("alias_doesnt_exist");
            fail("Should raise an exception because alias doesn't exist.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getRemove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetRemoveAReservedAlias() throws QuasardbException {
        try {
            qdbInstance.getRemove("qdb.test");
            fail("Should raise an exception because alias is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception message should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }

    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapANullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap(null, "test", "test");
            fail("Should raise an exception because alias is null");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
        
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapANullValueMeansException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("test1", null, "test");
            fail("Should raise an exception because value is null");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapANullComparandMeansException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("test", "test", null);
            fail("Should raise an exception because comparand is null");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapAEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("", "test", "test");
            fail("Should raise an exception because key is empty");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapAReservedKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("qdb.test", "test", "test");
            fail("Should raise an exception because key is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Message exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
     
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapAPojoWithSwapCase() throws QuasardbException {
        qdbInstance.purgeAll();
        
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo);
        assertTrue("Pojo " + pojoresult + " should be equals to Pojo " + pojo, pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojoGet + " should be equals to Pojo " + pojo2, pojo2.getText().equals(pojoGet.getText()));
        assertFalse("Pojo " + pojoGet + " shouldn't be equals to Pojo " + pojo, pojo.getText().equals(pojoGet.getText()));
        
        // Cleanup
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapAPojoWithNoSwapCase() throws QuasardbException {
        qdbInstance.purgeAll();
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        Pojo pojoresult = null;
        Pojo pojoGet = null;

        qdbInstance.put("test_nominal", pojo);
        Pojo pojo3 = new Pojo();
        pojo3.setText("test3");
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo3);
        assertFalse(pojo3.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo.getText().equals(pojoGet.getText()));
        
        // Cleanup
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapAWrongKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("alias_doesnt_exist", "test","test");
            fail("Should raise an exception because alias doesn't exist.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryANullKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap(null, "test", "test", 0);
            fail("Should raise an exception because alias is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryANullValueMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("test1", null, "test", 0);
            fail("Should raise an exception because value is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryANullComparandMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("test", "test", null, 0);
            fail("Should raise an exception because comparand is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryAnEmptyKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("", "test", "test", 0);
            fail("Should raise an exception because key is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryANegativeExpiryMeansAnException() throws QuasardbException {     
        // Test 1.5 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("test", "test", "test", -1);
            fail("Should raise an exception because a negative expiry is provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryAReservedKeyMeansAnException() throws QuasardbException {
        // Test 1.6 : wrong alias -> alias is reserved
        try {
            qdbInstance.compareAndSwap("qdb.test", "test", "test", 0);
            fail("Should raise an exception because a reserved key is provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpirySwapCaseWithInfiniteExpiryTime() throws QuasardbException {
        // Init case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        
        // Test case
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo, 0);
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoresult, pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojo2 + " should be equals to Pojo " + pojoGet, pojo2.getText().equals(pojoGet.getText()));
        assertFalse("Pojo " + pojo + " shouldn't be equals to Pojo " + pojoGet, pojo.getText().equals(pojoGet.getText()));
        
        // Cleanup
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryNoSwapWithCaseWithInfiniteExpiryTime() throws QuasardbException {
        // Init case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        Pojo pojo3 = new Pojo();
        pojo3.setText("test3");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = null;
        Pojo pojoGet = null;
        
        // Test case
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo3, 0);
        assertFalse("Pojo " + pojo3 + " shouldn't be equals to Pojo " + pojoresult, pojo3.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoGet, pojo.getText().equals(pojoGet.getText()));

        // Cleanup
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryAWrongKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.compareAndSwap("alias_doesnt_exist", "test","test", 0);
            fail("Should raise an exception because provided key doesn't exist.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpirySwapCaseWithOneSecondExpiryTime() throws QuasardbException {
        // Init case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = null;
        Pojo pojoGet = null;
        
        // Test case
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo, 2);
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoresult, pojo.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojo2 + " should be equals to Pojo " + pojoGet, pojo2.getText().equals(pojoGet.getText()));
        assertFalse("Pojo " + pojo + " shouldn't be equals to Pojo " + pojoGet, pojo.getText().equals(pojoGet.getText()));
        try {
            Thread.sleep(2500);
        } catch (Exception e) {
            fail("Shouldn't raise an exception because it's just a pause of 2.5 seconds.");
        }
        try {
            qdbInstance.get("test_nominal");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        
        // Cleanup
        pojoresult = null;
        pojoGet = null;
    }
    
    /**
     * Test of method {@link Quasardb#compareAndSwap(String, V, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testCompareAndSwapWithExpiryNoSwapCaseWithOneSecondExpiryTime() throws QuasardbException { 
        // Init case
        qdbInstance.purgeAll();
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        Pojo pojo3 = new Pojo();
        pojo3.setText("test3");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = null;
        Pojo pojoGet = null;
        
        // Test case
        qdbInstance.update("test_nominal", pojo);
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo3, 2);
        assertFalse("Pojo " + pojo3 + " shouldn't be equals to Pojo " + pojoresult, pojo3.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoGet, pojo.getText().equals(pojoGet.getText()));
        try {
            Thread.sleep(2500);
        } catch (Exception e) {
            fail("Shouldn't raise an exception because it's just a pause of 2.5 seconds.");
        }
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoGet, pojo.getText().equals(pojoGet.getText()));
        
        // Cleanup
        pojoresult = null;
        pojoGet = null;
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of method {@link Quasardb#remove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveANullKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.remove(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#remove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveAnEmptyKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.remove("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#remove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveAWrongKeyMeansException() throws QuasardbException {
        assertFalse("Should return false because alias doesn't exist.", qdbInstance.remove("alias_doesnt_exist"));
    }

    /**
     * Test of method {@link Quasardb#remove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveAReservedKeyMeansException() throws QuasardbException {
        assertFalse("Should return false because you cannot remove a reserved entry.", qdbInstance.remove("qdb.test"));
    }
    
    /**
     * Test of method {@link Quasardb#remove(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemove() throws QuasardbException {
        String test = "Voici un super test";
        qdbInstance.update("test_del_1", test);
        assertTrue("Should return true because entry was successfully removed.", qdbInstance.remove("test_del_1"));
        try {
            qdbInstance.get("test_del_1");
            fail("Should raise an exception because entry should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#purgeAll()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPurgeAll() throws QuasardbException {
        // Test : nominal case - add 4 objects and remove them all
        String test = "Voici un super test";
        qdbInstance.update("test_del_1", test);
        qdbInstance.update("test_del_2", test);
        qdbInstance.update("test_del_3", test);
        qdbInstance.update("test_del_4", test);
        qdbInstance.purgeAll();
        try {
            qdbInstance.get("test_del_1");
            fail("Should raise an exception because entry should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_2");
            fail("Should raise an exception because entry should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_3");
            fail("Should raise an exception because entry should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_4");
            fail("Should raise an exception because entry should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfANullKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.removeIf(null, "test");
            fail("Should raise an exception because null key is provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
     
    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfANullComparandMeansAnException() throws QuasardbException {
        try {
            qdbInstance.removeIf("test1", null);
            fail("Should raise an exception because null comparand is provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfNullParamsMeansAnException() throws QuasardbException {
        try {
            qdbInstance.removeIf(null, null);
            fail("Should raise an exception because null params are provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfAReservedKeyMeansAnException() throws QuasardbException {
        assertFalse("Should return false because provided key is reserved", qdbInstance.removeIf("qdb.test", "test"));
    }
     
    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfRemoveCase() throws QuasardbException {
        // Init case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test1");
        qdbInstance.put("test_nominal", pojo);
        
        // Test case
        assertTrue("Entry should have been removed.", qdbInstance.removeIf("test_nominal", pojo2));
        try {
            qdbInstance.get("test_nominal");
            fail("Should raise an exception because provided key should have been removed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfNoRemoveCase() throws QuasardbException {
        // Init case
        Pojo pojo = null;
        Pojo pojo2 = null;
        pojo = new Pojo();
        pojo.setText("test1");
        pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        
        // Test case
        assertFalse("Entry shouldn't have been removed.", qdbInstance.removeIf("test_nominal", pojo2));
        Pojo pojoResult = qdbInstance.get("test_nominal");
        assertTrue("Pojo " + pojoResult + " should be equals to Pojo " + pojo, pojoResult.getText().equals(pojo.getText()));

        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of method {@link Quasardb#removeIf(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRemoveIfWrongKeyMeansAnException() throws QuasardbException {
        assertFalse("Should return false because provided key doesn't exist.", qdbInstance.removeIf("alias_doesnt_exist", "test"));
    }

    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateANullValueMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update("test_update_1", null);
            fail("Should raise an exception because value is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateANullKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update(null, "test_update_1");
            fail("Should raise an exception because provided key is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateNullParamsMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update(null, null);
            fail("Should raise an exception because no params are provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateAReservedKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update("qdb.test", "test");
            fail("Should raise an exception because provided key is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
        
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateASimpleObject() throws QuasardbException {
        // Init case
        String test = "Voici un super test";
        
        // Test case
        assertTrue("Update should be successfull", qdbInstance.update("test_update_1", test));
        String result = qdbInstance.get("test_update_1");
        assertTrue("String " + test + " should be equals to " + result, test.equals(result));
        
        // Cleanup
        qdbInstance.remove("test_update_1");
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateAPojo() throws QuasardbException {
        // Init case
        Pojo pojo = new Pojo();
        
        // Test case
        assertTrue("Update should be successfull", qdbInstance.update("test_update_2", pojo));
        Pojo pojoresult = qdbInstance.get("test_update_2");
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoresult, pojo.getText().equals(pojoresult.getText()));

        // Cleanup
        qdbInstance.remove("test_update_2");
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryANullValueMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update("test_update_1", null, 0);
            fail("Should raise an exception because provided value is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryANullKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update(null, "test_update_1", 0);
            fail("Should raise an exception because provided key is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryNullParamsMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update(null, null, 0);
            fail("Should raise an exception because params are null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryANegativeExpiryMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update("test_update_1", "negative value", -1);
            fail("Should raise an exception because expiry param is negative.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryAReservedKeyMeansAnException() throws QuasardbException {
        try {
            qdbInstance.update("qdb.test", "test", 0);
            fail("Should raise an exception because key is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
        
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryASimpleObject() throws QuasardbException {
        String test = "Voici un super test";
        assertTrue("Update should be successfull", qdbInstance.update("test_update_1", test, 5));
        String result = qdbInstance.get("test_update_1");
        assertTrue("String " + test + " should be equals to " + result, test.equals(result));
        
        // Cleanup
        qdbInstance.remove("test_update_1");
    }
        
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiryAPojo() throws QuasardbException {
        Pojo pojo = new Pojo();
        assertTrue("Update should be successfull", qdbInstance.update("test_update_2", pojo, 0));
        Pojo pojoresult = qdbInstance.get("test_update_2");
        assertTrue("Pojo " + pojo + " should be equals to Pojo " + pojoresult, pojo.getText().equals(pojoresult.getText()));
        
        // Cleanup
        qdbInstance.remove("test_update_2");
    }
    
    /**
     * Test of method {@link Quasardb#update(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testUpdateWithExpiry() throws QuasardbException {
        Pojo pojo = new Pojo();
        qdbInstance.put("test_update_3", pojo);
        assertTrue("Update should be successfull.", qdbInstance.update("test_update_3", "expiry_test", 2));
        String resultat = qdbInstance.get("test_update_3");
        assertTrue("Entry should be here => no expiry yet (1 second)", resultat.equals("expiry_test"));
        try {
            Thread.sleep(2500);
        } catch (Exception e) {
            fail("No exception here.");
        }
        try {
            qdbInstance.get("test_update_3");
            fail("Entry should have been removed (expiry after waiting 1.8 seconds)");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#Quasardb(QuasardbConfig)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testQuasardbConstructorWithErrors() throws QuasardbException {
        QuasardbConfig config = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode("unknown_host", PORT);
        config.addNode(node);
        try {
            new Quasardb(config);
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link QuasardbException#QuasardbException()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testQuasardbException() throws QuasardbException {
        QuasardbException exception = new QuasardbException();
        assertNull("Exception message must be null", exception.getMessage());
    }

    /**
     * Test of method {@link Quasardb#getCurrentNodeConfig()}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#getNodeConfig(String, int)}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#getCurrentNodeStatus()}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#getNodeStatus(String, int)}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#getCurrentNodeTopology()}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#getCurrentNodeTopology(String, int)}.
     * 
     * @throws QuasardbException
     */
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
     * Test of method {@link Quasardb#iterator()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIteratorOn100Entries() throws QuasardbException {
        int nbIterations = 100;
        try {
            // Populate quasardb instance
            for (int i = 0; i < nbIterations; i++) {
                Pojo p = new Pojo();
                p.setText("test iterator " + i);
                qdbInstance.put("test_iterator_" + i, p);
            }
            
            // Iterate quasardb instance
            int i = 0;
            for (QuasardbEntry<?> qdbe : qdbInstance) {
                assertTrue("Key " + qdbe.getAlias() + " should be equals to its value " + qdbe.getValue(), qdbInstance.get(qdbe.getAlias()).equals(qdbe.getValue()));
                i++;
            }
            assertTrue("Nb iterations [" + i + "] should be equals to " + nbIterations, i == nbIterations);
        } catch (Exception e) {
            fail("No exception allowed.");
        } finally {
            qdbInstance.purgeAll();
        }
    }
    
    /**
     * Test of method {@link Quasardb#iterator()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIteratorOnNoEntriesMeansHasNextIsFalse() throws QuasardbException {
        qdbInstance.purgeAll();
        assertFalse("hasNext() response should be equals to false.", qdbInstance.iterator().hasNext());
    }

    /**
     * Test of method {@link Quasardb#iterator()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIteratorOn1000Entries() throws QuasardbException {
        int nbIterations = 1000;
        try {
            // Populate quasardb instance
            for (int i = 0; i < nbIterations; i++) {
                Pojo p = new Pojo();
                p.setAbc(i);
                p.setText("test iterator " + i);
                qdbInstance.put("test_iterator_" + i, p);
            }
            
            // Iterate quasardb instance
            int i = 0;
            for (QuasardbEntry<?> qdbe : qdbInstance) {
                assertTrue("Alias shouldn't be null", qdbe.getAlias() != null);
                assertTrue("Value shouldn't be null", qdbe.getValue() != null);
                assertTrue("Value should be a Pojo => " + qdbe.getValue(), qdbe.getValue() instanceof Pojo);
                i++;
            }
            assertTrue("Nb iterations [" + i + "] should be equals to " + nbIterations, i == (nbIterations));
        } catch (Exception e) {
            fail("No exception allowed.");
        } finally {
            qdbInstance.purgeAll();
        }
    }

    /**
     * Test of method {@link Quasardb#getDefaultExpiryTimeInSeconds()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetDefaultExpiryTimeInSeconds() throws QuasardbException {
        assertTrue("Default expiry time should be equals to 0.", qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    }

    /**
     * Test of method {@link Quasardb#setDefaultExpiryTimeInSeconds(long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetDefaultExpiryTimeInSecondsSetValueTo20s() throws QuasardbException {
        long expiryTime = 20L;
        qdbInstance.setDefaultExpiryTimeInSeconds(expiryTime);
        assertTrue("Default expiry time should be equals to 20 => " + qdbInstance.getDefaultExpiryTimeInSeconds(), qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);
    }
    
    /**
     * Test of method {@link Quasardb#setDefaultExpiryTimeInSeconds(long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetDefaultExpiryTimeInSecondsSetValueToNoExpiry() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        assertTrue("Default expiry time should be equals to 0 => " + qdbInstance.getDefaultExpiryTimeInSeconds(), qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    }
    
    /**
     * Test of method {@link Quasardb#setDefaultExpiryTimeInSeconds(long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetDefaultExpiryTimeInSecondsANegativeValueMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setDefaultExpiryTimeInSeconds(-1);
            fail("Should raise an exception because provided value shouldn't be a negative value.");
        } catch (QuasardbException e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#setDefaultExpiryTimeInSeconds(long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetDefaultExpiryTimeInSecondsViaAProvidedConfig() throws QuasardbException {
        long expiryTime = 20L;
        QuasardbConfig config2 = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode(HOST, PORT);
        config2.addNode(node);
        config2.setExpiryTimeInSeconds(20);
        qdbInstance = new Quasardb(config2);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        assertTrue("Default expiry time should be equals to 20 => " + qdbInstance.getDefaultExpiryTimeInSeconds(), qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);

        // Clean up
        config2 = null;
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    }

    /**
     * Test of method {@link Quasardb#put(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPutWithExpiryTimeOneSecondValue() throws QuasardbException {
        long expiry = 2L;
        String test = "Voici un super test";
        qdbInstance.put("test_put_expiry_1", test, expiry);
        String result = qdbInstance.get("test_put_expiry_1");
        assertTrue("Alias test_put_expiry_1 should exists.", test.equals(result));
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_put_expiry_1");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPutWithExpiryTimeANegativeValueMeansAnException() throws QuasardbException {
        String test = "Voici un super test";
        try {
            qdbInstance.put("test_put_expiry_1", test, -1);
            fail("Should raise an exception because a negative value was provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#put(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPutWithExpiryTimeCurrentTimePlus2Seconds() throws QuasardbException {
        // Init case
        String test = "Voici un super test";
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        long computedExpiry = cal.getTimeInMillis() / 1000;
        qdbInstance.put("test_put_expiry_2", test, 2L);
        assertTrue("Expiry time " + qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") + " should be equals to computed expiry time " + computedExpiry, qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") == computedExpiry);
        
        // Wait for expiry
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }

        // Test expiry
        try {
            qdbInstance.get("test_put_expiry_2");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#put(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPutWithExpiryTimeDifferentExpiryTimes() throws QuasardbException {
        // Init case
        long expiry = 2L;
        String test = "Voici un super test";
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.put("test_put_expiry_2", test);
        assertTrue("Expiry time should be equals to 0 => " + qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2"), qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") == 0);
        qdbInstance.put("test_put_expiry_3", test, expiry);
        qdbInstance.put("test_put_expiry_4", test, 3 * expiry);
        try {
            Thread.sleep((expiry + (expiry / 2)) * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue("Entry[test_put_expiry_2] should be here => no expiry", ((String) qdbInstance.get("test_put_expiry_2")).equalsIgnoreCase(test));
        try {
            qdbInstance.get("test_put_expiry_3");
            fail("Should raise an exception because entry [test_put_expiry_3] should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        assertTrue("Entry[test_put_expiry_2] should be here => expiry time was " + 3 * expiry, ((String) qdbInstance.get("test_put_expiry_4")).equalsIgnoreCase(test));
        try {
            Thread.sleep(3 * expiry * 1000);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_put_expiry_4");
            fail("Should raise an exception because entry [test_put_expiry_4] should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        
        // Clean up
        qdbInstance.remove("test_put_expiry_2");
    }
    
    /**
     * Test of method {@link Quasardb#put(String, V, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testPutWithExpiryTimeAReservedAliasMeansAnException() throws QuasardbException {
        try {
            long expiry = 1L;
            qdbInstance.put("qdb.test", "test", expiry);
            fail("Should raise an exception because entry is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias => " + ((QuasardbException) e).getCode(), ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeInSeconds(String, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeInSeconds() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.purgeAll();
        String test = "Voici un super test";
        qdbInstance.put("test_expiry_1", test);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        assertTrue("Entry [test_expiry_1] should exist because there is no expiry time.", ((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", 1L);
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }
        try {
            qdbInstance.get("test_expiry_1");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }

        // Test with +2 seconds expiry time
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        long computedExpiry = cal.getTimeInMillis() / 1000;
        qdbInstance.put("test_expiry_1", test, 2L);
        assertTrue("Expiry time " + qdbInstance.getExpiryTimeInSeconds("test_expiry_1") + " should be equals to computed expiry time " + computedExpiry, qdbInstance.getExpiryTimeInSeconds("test_expiry_1") == computedExpiry);
        
        // Wait for expiry
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }

        // Test expiry
        try {
            qdbInstance.get("test_expiry_1");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#setExpiryTimeInSeconds(String, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeInSecondsNegativeParamMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeInSeconds("test_expiry_1", -1);
            fail("Should raise an exception because a negative expiry time was provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeInSeconds(String, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeInSecondsWrongAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeInSeconds("wrong_alias", 1L);
            fail("Should raise an exception because entry was invalid.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeInSeconds(String, long)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeInSecondsNullAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeInSeconds(null, 1L);
            fail("Should raise an exception because entry was null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getExpiryTimeInSeconds(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInSeconds2SecondsExpiry() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.purgeAll();
        
        String test = "Voici un super test";
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        long computedExpiry = cal.getTimeInMillis() / 1000;
        qdbInstance.put("test_expiry_1", test, 2L);
        assertTrue("Expiry time " + qdbInstance.getExpiryTimeInSeconds("test_expiry_1") + " should be equals to computed expiry time " + computedExpiry, qdbInstance.getExpiryTimeInSeconds("test_expiry_1") == computedExpiry);

        // Wait for expiry
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e1) {
            fail("No exception allowed.");
        }

        // Test expiry
        try {
            qdbInstance.getExpiryTimeInSeconds("test_expiry_1");
            fail("Should raise an exception because entry should have been expired.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getExpiryTimeInSeconds(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInSecondsNoExpiry() throws QuasardbException {
        qdbInstance.update("test_expiry_1", new String("testGetExpiryTimeInSecondsNoExpiry"));
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", 0);
        try {
             Thread.sleep(1L * 1000 + 500);
        } catch (InterruptedException e1) {
             fail("No exception allowed.");
        }
        assertEquals("Expiry for Entry[test_expiry_1] should be equals to 0.", qdbInstance.getExpiryTimeInSeconds("test_expiry_1"), 0);
    }
    
    /**
     * Test of method {@link Quasardb#getExpiryTimeInSeconds(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInSecondsAWrongAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInSeconds("wrong_alias");
            fail("Should raise an exception because entry doesn't exist.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getExpiryTimeInSeconds(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInSecondsANullAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInSeconds(null);
            fail("Should raise an exception because entry is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getExpiryTimeInSeconds(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInSecondsAnEmptyAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInSeconds("");
            fail("Should raise an exception because entry is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeAt(String, Date)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeAtNullAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeAt(null, new Date());
            fail("Should raise an exception because entry is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#setExpiryTimeAt(String, Date)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeAtANullDateMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeAt("test_expiry_1", null);
            fail("Should raise an exception because date is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#setExpiryTimeAt(String, Date)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeAtAnEmptyEntryMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeAt("", new Date());
            fail("Should raise an exception because entry is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeAt(String, Date)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeAtAWrongAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.setExpiryTimeAt("wrong_alias", new Date());
            fail("Should raise an exception because entry doesn't exist.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#setExpiryTimeAt(String, Date)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testSetExpiryTimeAt() throws QuasardbException {
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.purgeAll();
        String test = "Voici un super test";
        qdbInstance.put("test_expiry_1", test);
        long time = System.currentTimeMillis() + (1000 * 60 * 60);
        Date expiryDate = new Date(time);
        qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
        String sTime = "" + time;
        assertTrue("Expiry time for entry[test_expiry_1] should be equals to computed date => " + sTime.substring(0, sTime.length() - 3), (qdbInstance.getExpiryTimeInSeconds("test_expiry_1") + "").equals(sTime.substring(0, sTime.length() - 3)));

        // Clean up 
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#getExpiryTimeInDate(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInDateANullParamMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInDate(null);
            fail("Should raise an exception because entry is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#getExpiryTimeInDate(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInDateAnEmptyAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInDate("");
            fail("Should raise an exception because entry is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getExpiryTimeInDate(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInDateAWrongAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getExpiryTimeInDate("wrong_alias");
            fail("Should raise an exception because entry is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#getExpiryTimeInDate(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetExpiryTimeInDate() throws QuasardbException {
        long time = System.currentTimeMillis() + (1000 * 60 * 60);
        Date expiryDate = new Date(time);
        qdbInstance.update("test_expiry_1", new String("test_expiry_1"));
        qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
        assertTrue("Expiry data for entry[test_expiry_1] should be equals to computed date => " + expiryDate, qdbInstance.getExpiryTimeInDate("test_expiry_1").toString().equals(expiryDate.toString()));

        // Cleanup Qdb
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#startsWith(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testStartsWithANullPrefixMeansAnException() throws QuasardbException {
        try {
            qdbInstance.startsWith(null);
            fail("Should raise an exception because prefix is null.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#startsWith(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testStartsWithAnEmptyPrefixMeansAnException() throws QuasardbException {
        try {
            qdbInstance.startsWith("");
            fail("Should raise an exception because prefix is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }

    /**
     * Test of method {@link Quasardb#startsWith(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testStartsWithNoPrefixMatch() throws QuasardbException {
        List<String> result = null;
        try {
            result = qdbInstance.startsWith("unknow_prefix");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
        assertTrue("Result List should be null.", result == null);
    }

    /**
     * Test of method {@link Quasardb#startsWith(String)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testStartsWith() throws QuasardbException {
        qdbInstance.purgeAll();
        
        // Init case
        for (int i = 0; i < 10; i++) {
            Pojo value = new Pojo();
            value.setText("test number " + i);
            qdbInstance.put("prefix1.startWith_" + i, value);
            if (i % 2 == 0) {
                qdbInstance.put("prefix2.startWith_" + i, value);
            }
        }
        
        // Test case
        List<String> result = qdbInstance.startsWith("prefix1");
        assertTrue(result.size() == 10);
        for (int i = 0; i < 10; i++) {
            assertTrue("Alias for index " + i + " should be equals to prefix1.startWith_" + i + " => " + result.get(i), result.get(i).equalsIgnoreCase("prefix1.startWith_" + i));
            assertTrue("Value at index " + i + " should be equals to " + "test number " + i, ((Pojo) qdbInstance.get(result.get(i))).getText().equalsIgnoreCase("test number " + i));
        }
        result = null;
        result = qdbInstance.startsWith("prefix2");
        assertTrue("Number of entries prefixed by [prefix2] should be equals to 5.", result.size() == 5);
        for (int i = 0; i < 5; i++) {
            assertTrue("Alias for index " + i + " should be equals to prefix2.startWith_" + (i*2) + " => " + result.get(i), result.get(i).equalsIgnoreCase("prefix2.startWith_" + (i*2)));
            assertTrue("Value at index " + i + " should be equals to test number " + (i*2), ((Pojo) qdbInstance.get(result.get(i))).getText().equalsIgnoreCase("test number " + (i*2)));
        }
        
        // Clean up
        result = null;
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchNullParamMeansAnException() throws QuasardbException {
        qdbInstance.purgeAll();
        try {
            qdbInstance.runBatch(null);
            fail("Should raise an exception because param is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGets() throws QuasardbException {
        Pojo testGet = new Pojo();
        testGet.setText("test_batch_get");
        qdbInstance.update("test_batch_get", testGet);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        Operation<Pojo> operationGet = new Operation<Pojo>(TypeOperation.GET, "test_batch_get");
        operations.add(operationGet);
        Results results = qdbInstance.runBatch(operations);
        assertTrue("All batch operations should have been successfull.", results.isSuccess());
        assertTrue("Results for batch operation shouldn't be empty", !results.getResults().isEmpty());
        for (Result<?> result : results.getResults()) {
            assertTrue("Alias should be equals to 'test_batch_get'.", result.getAlias().equals("test_batch_get"));
            assertTrue("Operations in batch should be a GET operation.", result.getTypeOperation() == TypeOperation.GET);
            assertTrue("Current operation should have no error.", result.getError() == null);
            assertTrue("POJO value for current GET operation should be equals to 'test_batch_get'.", ((Pojo) result.getValue()).getText().equals("test_batch_get"));
        }
        operations = null;
        results = null;
        qdbInstance.purgeAll();
    }
    
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchPuts() throws QuasardbException {
        //  -> Test 2.2 : nominal case -> PUT
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_put");
        Operation<Pojo> operationPut = new Operation<Pojo>(TypeOperation.PUT, "test_batch_put", testPut);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationPut);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchUpdates() throws QuasardbException {
        //  -> Test 2.3 : nominal case -> UPDATE
        qdbInstance.update("test_batch_update", "test");
        assertTrue(((String) qdbInstance.get("test_batch_update")).equals("test"));
        Pojo testUpdate = new Pojo();
        testUpdate.setText("test_batch_update");
        Operation<Pojo> operationUpdate = new Operation<Pojo>(TypeOperation.UPDATE, "test_batch_update", testUpdate);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationUpdate);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchRemoves() throws QuasardbException {
        //  -> Test 2.4 : nominal case -> REMOVE
        Pojo testRemove = new Pojo();
        testRemove.setText("test_batch_remove");
        qdbInstance.put("test_batch_remove", testRemove);
        Operation<Pojo> operationRemove = new Operation<Pojo>(TypeOperation.REMOVE, "test_batch_remove");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemove);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchCAS() throws QuasardbException {
        //  -> Test 2.5 : nominal case -> CAS
        Pojo testCas1 = new Pojo();
        testCas1.setText("test_batch_cas");
        qdbInstance.update("test_batch_cas", testCas1);
        assertTrue(((Pojo) qdbInstance.get("test_batch_cas")).getText().equals("test_batch_cas"));
        Pojo testCas2 = new Pojo();
        testCas2.setText("test_batch_cas_2");

        Operation<Pojo> operationCas = new Operation<Pojo>(TypeOperation.CAS, "test_batch_cas", testCas1, testCas2);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationCas);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGetRemoves() throws QuasardbException {
        //  -> Test 2.6 : nominal case -> GET REMOVE
        Pojo testGetRemove = new Pojo();
        testGetRemove.setText("test_batch_get_remove");
        qdbInstance.put("test_batch_get_remove", testGetRemove);
        assertTrue(((Pojo) qdbInstance.get("test_batch_get_remove")).getText().equals("test_batch_get_remove"));

        Operation<Pojo> operationGetRemove = new Operation<Pojo>(TypeOperation.GET_REMOVE, "test_batch_get_remove");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetRemove);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGetUpdates() throws QuasardbException {
        //  -> Test 2.7 : nominal case -> GET UPDATE
        Pojo testGetUpdate = new Pojo();
        testGetUpdate.setText("test_batch_get_update");
        qdbInstance.update("test_batch_get_update", testGetUpdate);
        assertTrue(((Pojo) qdbInstance.get("test_batch_get_update")).getText().equals("test_batch_get_update"));
        Pojo testGetUpdate2 = new Pojo();
        testGetUpdate2.setText("test_batch_get_update_2");

        Operation<Pojo> operationGetUpdate = new Operation<Pojo>(TypeOperation.GET_UPDATE, "test_batch_get_update", testGetUpdate2);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetUpdate);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchRemoveIfs() throws QuasardbException {
        //  -> Test 2.8 : nominal case -> REMOVE IF
        Pojo testRemoveIf = new Pojo();
        testRemoveIf.setText("test_batch_remove_if");
        qdbInstance.put("test_batch_remove_if", testRemoveIf);
        Operation<Pojo> operationRemoveIf = new Operation<Pojo>(TypeOperation.REMOVE_IF, "test_batch_remove_if", testRemoveIf);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemoveIf);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGetsWithFailedOperations() throws QuasardbException {
        // Test 3 : Failed batch operations
        //  -> Test 3.1 : Fail a GET
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        Operation<Pojo> operationGet = new Operation<Pojo>(TypeOperation.GET, "test_batch_get");
        operations.add(operationGet);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchPutsWithFailedOperations() throws QuasardbException {
        //  -> Test 3.2 : Fail a PUT
        Operation<Pojo> operationPut = new Operation<Pojo>(TypeOperation.PUT, "test_batch_put", null);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationPut);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchUpdatesWithFailedOperations() throws QuasardbException {
        //  -> Test 3.3 : Fail a UPDATE
        Operation<Pojo> operationUpdate = new Operation<Pojo>(TypeOperation.UPDATE, "test_batch_update", null);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationUpdate);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchRemovesWithFailedOperations() throws QuasardbException {
        //  -> Test 3.4 : Fail a REMOVE
        Operation<Pojo> operationRemove = new Operation<Pojo>(TypeOperation.REMOVE, "test_batch_remove");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemove);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchCASWithFailedOperations() throws QuasardbException {
        Pojo testCas1 = new Pojo();
        testCas1.setText("test_batch_cas");
        qdbInstance.update("test_batch_cas", testCas1);
        assertTrue(((Pojo) qdbInstance.get("test_batch_cas")).getText().equals("test_batch_cas"));
        Pojo testCas2 = new Pojo();
        testCas2.setText("test_batch_cas_2");
        
        qdbInstance.update("test_batch_cas", testCas1);
        assertTrue(((Pojo) qdbInstance.get("test_batch_cas")).getText().equals("test_batch_cas"));
        Operation<Pojo> operationCas = new Operation<Pojo>(TypeOperation.CAS, "test_batch_cas_fail", testCas1, testCas2);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationCas);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGetRemovesWithFailedOperations() throws QuasardbException {
        Operation<Pojo> operationGetRemove = new Operation<Pojo>(TypeOperation.GET_REMOVE, "test_batch_get_remove");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetRemove);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchGetUpdatesWithFailedOperations() throws QuasardbException {
        Pojo testGetUpdate2 = new Pojo();
        testGetUpdate2.setText("test_batch_get_update_2");
        Operation<Pojo> operationGetUpdate = new Operation<Pojo>(TypeOperation.GET_UPDATE, "test_batch_get_update", testGetUpdate2);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationGetUpdate);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchRemoveIfsWithFailedOperations() throws QuasardbException {
        Pojo testRemoveIf = new Pojo();
        testRemoveIf.setText("test_batch_remove_if");
        Operation<Pojo> operationRemoveIf = new Operation<Pojo>(TypeOperation.REMOVE_IF, "test_batch_remove_if", testRemoveIf);
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        operations.add(operationRemoveIf);
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchSimplePutsOperations() throws QuasardbException {
        // Test 4 : simple successful put operations
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            operations.add(operation);
        }
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchMultipleOperations() throws QuasardbException {
        // Test 5 : multiple successful operations
        Pojo test = new Pojo();
        test.setText("test_batch");
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_put");
        Pojo testGetUpdate = new Pojo();
        testGetUpdate.setText("test_batch_get_update");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            qdbInstance.update( "test_batch_" + i, test);
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
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchtestRunBatchMultipleOperationsWithFailedOperations() throws QuasardbException {
        // Test 6 : simple put operations with entry errors => all operations are in error.
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            if (i % 2 != 0) {
                operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_error_" + i, null);
            }
            operations.add(operation);
        }
        Results results = qdbInstance.runBatch(operations);
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
        qdbInstance.purgeAll();
    }

    /**
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchtestRunBatchMultiplePutsWithFailedOperations() throws QuasardbException {
        // Test 7 : simple put operations with legal errors
        qdbInstance.purgeAll();
        
        Pojo testPut = new Pojo();
        testPut.setText("test_batch_simple_put");
        List<Operation<Pojo>> operations = new ArrayList<Operation<Pojo>>();
        for (int i = 0; i < 300; i++) {
            Operation<Pojo> operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + i, testPut);
            if (i % 2 != 0) {
                operation = new Operation<Pojo>(TypeOperation.PUT, "test_batch_simple_" + (i - 1), testPut);
            }
            operations.add(operation);
        }
        Results results = qdbInstance.runBatch(operations);
        assertTrue(!results.isSuccess());
        assertTrue(!results.getResults().isEmpty());
        int it = 0;
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
        qdbInstance.purgeAll();
    }
    
    /**
     * Test of class {@link QuasardbConfig}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testMultiConnectWithAWrongConfigurationMeansAnException() throws QuasardbException {
        QuasardbConfig conf = new QuasardbConfig();
        QuasardbNode node1 = new QuasardbNode(HOST, 12345);
        QuasardbNode node2 = new QuasardbNode("unknown_host2", PORT);
        conf.addNode(node1);
        conf.addNode(node2);
        
        // Test 1 : wrong ring provided
        Quasardb qdb = new Quasardb(conf);
        try {
            qdb.connect();
            fail("An exception must be thrown => wrong config provided");
        } catch (QuasardbException e) {
            assertTrue(e.getMessage().startsWith("Wrong config provided"));
        } finally {
            if (qdb != null) {
                try {
                    qdb.close();
                } catch (Exception e) {}
            }
        }
        qdb = null;
    }
     
    /**
     * Test of class {@link QuasardbConfig}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testMultiConnect() throws QuasardbException {
        // Test 2 : wrong nodes and one right node
        QuasardbConfig conf = new QuasardbConfig();
        QuasardbNode node3 = new QuasardbNode(HOST, PORT);
        conf.addNode(node3);
        Quasardb qdb = new Quasardb(conf);
        try {
            qdb.connect();
            qdb.put("conf", conf);
            QuasardbConfig qdbConf = qdb.get("conf");
            assertTrue(qdbConf.getNodes().size() == conf.getNodes().size());
            
            qdb.purgeAll();
        } catch (Exception e) {
            fail("No exception allowed => node " + node3.getHostName() + ":" + node3.getPort() + " must be OK.");
        }
        
        conf = null;
        qdb = null;
    }
}
