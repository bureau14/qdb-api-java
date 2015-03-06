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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;

import java.util.Date;
import java.util.List;

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

import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link Quasardb} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.6
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    LibraryHelper.class,
    qdbJNI.class,
    qdb_error_t.class
})
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
    public void setUp() throws QuasardbException {
        PowerMockito.mockStatic(LibraryHelper.class);
        PowerMockito.mockStatic(qdbJNI.class);
        PowerMockito.mockStatic(qdb_error_t.class);
        PowerMockito.mockStatic(qdb.class);

        BDDMockito.given(qdb_error_t.swigToEnum(any(int.class))).willReturn(qdb_error_t.error_ok);
        BDDMockito.given(qdbJNI.open()).willReturn(123L);
        BDDMockito.given(qdbJNI.connect(any(long.class), any(String.class), any(int.class))).willReturn(1);
        BDDMockito.given(qdbJNI.close(any(long.class))).willReturn(1);
        
        qdbInstance = new Quasardb(config);
        qdbInstance.connect();
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
        qdbInstance.close();
        try {
            qdbInstance.put("test_close", "test_close");
            fail("Should raise an Exception because session is closed => no more operations are allowed.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }

        // re-initialize qdb session
        setUp();
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
        qdbInstance.connect();
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
        qdbInstance.connect();
        try {
            qdbInstance.getAndReplace("qdb.test", "test", 0);
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
    public void testGetAndReplaceWithExpiryANegativeExpiryTimeMeansException() throws QuasardbException {
        try {
            qdbInstance.getAndReplace("test", "test", -1);
            fail("Should raise an Exception because expiry time must be a positive value.");
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
        qdbInstance.connect();
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
        qdbInstance.connect();
        try {
            qdbInstance.compareAndSwap("qdb.test", "test", "test");
            fail("Should raise an exception because key is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Message exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
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
        qdbInstance.connect();
        try {
            qdbInstance.compareAndSwap("qdb.test", "test", "test", 0);
            fail("Should raise an exception because a reserved key is provided.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
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
    public void testRemoveAReservedKeyMeansException() throws QuasardbException {
        try {
            qdbInstance.remove("qdb.test");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
            assertTrue("Exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
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
        try {
            assertFalse("Should return false because provided key is reserved", qdbInstance.removeIf("qdb.test", "test"));
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
            assertTrue("Exception should be a error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
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
        qdbInstance.connect();
        try {
            qdbInstance.update("qdb.test", "test");
            fail("Should raise an exception because provided key is reserved.");
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
        qdbInstance.connect();
        try {
            qdbInstance.update("qdb.test", "test", 0);
            fail("Should raise an exception because key is reserved.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias code", ((QuasardbException) e).getCode().equals("error_reserved_alias"));
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
    public void testPutWithExpiryTimeAReservedAliasMeansAnException() throws QuasardbException {
        qdbInstance.connect();
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
     * Test of method {@link Quasardb#runBatch(List)}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testRunBatchNullParamMeansAnException() throws QuasardbException {
        try {
            qdbInstance.runBatch(null);
            fail("Should raise an exception because param is empty.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of class {@link Quasardb#isConnected()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIsConnectWithNoSessionsMeansFalse() throws QuasardbException {
        Quasardb qdbWithNoSession = new Quasardb();
        assertFalse("qdbWithNoSession shouldn't be connected", qdbWithNoSession.isConnected());
    }
    
    /**
     * Test of class {@link Quasardb#isConnected()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIsConnectWithWrongConfigMeansFalse() throws QuasardbException {
        QuasardbConfig conf = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode(HOST, 123456);
        conf.addNode(node);
        Quasardb qdbWithWrongConfig = new Quasardb(conf);
        assertFalse("qdbWithWrongConfig shouldn't be connected.", qdbWithWrongConfig.isConnected());
    }
    
    /**
     * Test of class {@link Quasardb#isConnected()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testIsConnectWithNoConnectionMeansFalse() throws QuasardbException {
        QuasardbConfig conf = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode(HOST, PORT);
        conf.addNode(node);
        Quasardb qdbWithNoConnection = new Quasardb(conf);
        assertFalse("qdbWithNoConnection shouldn't be connected.", qdbWithNoConnection.isConnected());
    }
    
    /**
     * Test of class {@link Quasardb#getLocation()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetLocationWithANullAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getLocation(null);
            fail("Should raise an Exception because provided alias was null.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of class {@link Quasardb#getLocation()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetLocationWithAnEmptyAliasMeansAnException() throws QuasardbException {
        try {
            qdbInstance.getLocation("");
            fail("Should raise an Exception because provided alias was empty.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of class {@link Quasardb#getLocation()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetLocationWithAWrongAliasMeansAnException() throws QuasardbException {
        qdbInstance.connect();
        try {
            qdbInstance.getLocation("foo_bar");
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
    }
    
    /**
     * Test of class {@link Quasardb#getLocation()}.
     * 
     * @throws QuasardbException
     */
    @Test
    public void testGetLocationWithAReservedAliasMeansAnException() throws QuasardbException {
        qdbInstance.connect();
        try {
            qdbInstance.getLocation("qdb.reserved_namespace");
            fail("Shouldn't raise an Exception.");
        } catch (Exception e) {
            assertTrue("Exception should be a QuasardbException => " + e, e instanceof QuasardbException);
            assertTrue("Exception code should be error_reserved_alias => " + ((QuasardbException) e).getCode(), ((QuasardbException) e).getCode().equals("error_reserved_alias"));
        }
    }
}
