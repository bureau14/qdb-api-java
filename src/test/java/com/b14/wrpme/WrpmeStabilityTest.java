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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.databene.benerator.anno.Generator;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.feed4junit.Feeder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.b14.tests.tools.CustomContiperfFileExecutionLogger;

@RunWith(Feeder.class)
public class QuasardbStabilityTest {
    private static final String WRPME_NAME = "testStability";
    private static final int NB_LOOPS = 1;
    private static final int NB_THREADS = 1;
    private static final int REQ_AVERAGE_EXECUTION_TIME = 1000;
    private static final String GENERATOR_NAME = "com.b14.qdb.data.DataGenerator";
    private static final Map<String,String> config = new HashMap<String,String>();
    private static final Quasardb qdb = new Quasardb();
    
    @Rule
    public ContiPerfRule i = new ContiPerfRule(new CustomContiperfFileExecutionLogger());
    
    public QuasardbStabilityTest() {
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        config.put("name", WRPME_NAME);
        config.put("host", QuasardbTest.HOST);
        config.put("port", QuasardbTest.PORT);
        try {
            qdb.setConfig(config); 
            qdb.connect();
        } catch (QuasardbException e) {
            System.err.println("Could not initialize Quasardb connection pool for Loader: " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try {
            if (qdb != null) {
                qdb.close();
            }
        } catch (QuasardbException e) {
        }
    }
    
    @Before
    public void init() throws Exception {
    }
    
    @After
    public void cleanUp() {
    }
    
    private void cleanKeyIfExist(String key) {
        // Clean up stored value
        try {
            qdb.remove(key);
        } catch (QuasardbException e) {
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void putTest(String key, Object value) {
        key += "_put";
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        } 
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> boolean checkIfEquals(final T obj1, final T obj2) {
        if (obj1 instanceof Object[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && (Arrays.deepEquals((T[]) obj1, (T[]) obj2)));
        } else if (obj1 instanceof boolean[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((boolean[]) obj1).length == ((boolean[]) obj2).length);
        } else if (obj1 instanceof byte[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((byte[]) obj1).length == ((byte[]) obj2).length);
        } else if (obj1 instanceof char[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((char[]) obj1).length == ((char[]) obj2).length);
        } else if (obj1 instanceof short[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((short[]) obj1).length == ((short[]) obj2).length);
        } else if (obj1 instanceof int[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((int[]) obj1).length == ((int[]) obj2).length);
        } else if (obj1 instanceof long[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((long[]) obj1).length == ((long[]) obj2).length);
        } else if (obj1 instanceof float[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((float[]) obj1).length == ((float[]) obj2).length);
        } else if (obj1 instanceof double[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((double[]) obj1).length == ((double[]) obj2).length);
        } else if (obj1.getClass().getName().contains("String")) {
            return obj1.toString().equals(obj2.toString());
        } else if (obj1 instanceof Comparable) {
            if (((Comparable) obj1).compareTo(obj2) != 0) {
                return obj1.toString().equals(obj2.toString());
            } else  {
                return true;
            }
        } else if (obj1 instanceof Number) {
            return obj1.toString().equals(obj2.toString());
        } else if (obj1 instanceof Collection<?>) {
            if (!((Collection<?>) obj1).containsAll(((Collection<?>) obj2))) {
                Iterator<?> it = ((Collection<?>) obj1).iterator();
                Iterator<?> it2 = ((Collection<?>) obj2).iterator();
                while(it.hasNext() && it2.hasNext()) {
                    return checkIfEquals(it.next(), it2.next());
                }
                return false;
            } else {
                return true;
            }
        } else {
            return obj1.equals(obj2);
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void getTest(String key, final Object value) {
        key += "_get";
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if provided value is equals to the stored one
            assertTrue("Objects are not equal", checkIfEquals(value, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot get key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void updateTest(String key, final Object value) {
        key += "_update";
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, "TEST_UPDATE");
            
            // Check if provided value is equals to the stored one
            String sTest = qdb.get(key);
            assertTrue(sTest.equals("TEST_UPDATE"));
            
            // Update the current key value
            qdb.update(key, value);
            
            // Check if provided value is equals to the updated stored value
            assertTrue("Object for key[" + key + "] was not updated.", checkIfEquals(value, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot update key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void deleteTest(String key, final Object value) {
        key += "_delete";
        cleanKeyIfExist(key);
        
        try {
            qdb.put(key, value);
            qdb.remove(key);
        } catch (QuasardbException e) {
            fail("Delete failed for key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        }
    }
}
