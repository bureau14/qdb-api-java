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

package com.b14.tests.tools.benchs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

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

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.QuasardbTest;
import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

@RunWith(Feeder.class)
public class QuasardbRawTest {
    private static final int NB_LOOPS = 1;
    private static final int NB_THREADS = 10;
    private static final int REQ_AVERAGE_EXECUTION_TIME = 1000;
    private static final String GENERATOR_NAME = "com.b14.qdb.data.ParallelDataGenerator";
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
    private static final QuasardbConfig config = new QuasardbConfig();
    
    private static SWIGTYPE_p_qdb_session session;
    
    private Quasardb qdbDB = new Quasardb();
    
    static {
        try {
            System.loadLibrary("qdb_java_api");
        } catch (UnsatisfiedLinkError e) {
            LibraryHelper.loadLibrairiesFromJar();
        }
    }
    
    @Rule 
    public ContiPerfRule rule = new ContiPerfRule();

    @BeforeClass
    public static void setUpClass() throws Exception {
        // **** INIT RAW API ****
        // Try to opening a qdb session
        session = qdb.open();
        
        // Try to connect to the qdb node
        qdb_error_t qdbError = qdb.connect(session, QuasardbTest.HOST, QuasardbTest.PORT);
        
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            System.out.println(qdbError);
            System.exit(0);
        }
        
        // **** INIT QDB API ****
        QuasardbNode node = new QuasardbNode(QuasardbTest.HOST, QuasardbTest.PORT);
        config.addNode(node);
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        // **** CLOSE RAW API ****
        qdb_error_t qdbError = qdb.close(session);
        if (qdbError != qdb_error_t.error_ok) {
            System.out.println(qdbError);
            System.exit(0);
        }
    }
    
    @Before
    public void init() {
        qdbDB.setConfig(config);
        try {
            qdbDB.connect();
        } catch (QuasardbException e) {
            System.err.println("Could not initialize Quasardb connection pool for Loader: " + e.toString());
            e.printStackTrace();
            return;
        }
    }
    
    @After
    public void cleanUp() {
        try {
            if (qdbDB != null) {
                qdbDB.close();
            }
        } catch (QuasardbException e) {
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public synchronized void rawPutGetUpdateDeleteTest(String key, Object value) {
        ByteBuffer bufferGet = null;
        try {
            error_carrier error = new error_carrier();
            key += "_" + Thread.currentThread().getId();
            
            buffer.put((value.toString()).getBytes());
            buffer.flip();
            
            qdb.remove(session, key);
            qdb.put(session, key, buffer, buffer.limit(), 0);
            
            buffer.clear();
            
            bufferGet = qdb.get_buffer(session, key, error);
            
            buffer.rewind();
            buffer.put(("TEST_KEY_UPDATE_" + value).getBytes());
            qdb.update(session, key, buffer, buffer.limit(), 0);
            buffer.clear();
            
            qdb.remove(session, key);
        } finally {
            if (bufferGet != null) {
                qdb.free_buffer(session, bufferGet);
            }
        }
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void putGetUpdateDeleteTest(String key, Object value) {
        key += "_" + Thread.currentThread().getId();
        
        // Try to clean up stored value if exists
        try {
            qdbDB.remove(key);
        } catch (QuasardbException e) {
        }
        
        try {
            // Insert the provided value at provided key
            qdbDB.put(key, value);
            
            // Check if a value has been stored at key
            assertTrue(qdbDB.get(key).equals(value));
            
            // Update value
            qdbDB.update(key, value + "_UPDATED");
            
            // Check if a value has been updated at key
            assertFalse(qdbDB.get(key).equals(value));
            assertTrue(qdbDB.get(key).equals(value + "_UPDATED"));
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
        } finally {
            // Clean up stored value
            try {
                qdbDB.remove(key);
            } catch (QuasardbException e) {
            }
        } 
    }
    
    public static void main(String... args) throws QuasardbException {
//      // Try to opening a qdb session
//      SWIGTYPE_p_wrp_me_session session = qdb.open();
//        
//        // Try to connect to the qdb node
//        wrp_me_error_t qdbError = qdb.connect(session, "127.0.0.1", 5909);
//        
//        // Handle errors
//        if (qdbError != wrp_me_error_t.error_ok) {
//            System.out.println(qdbError);
//            System.exit(0);
//        }
//        
//        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        String alias = "alias_";
        String key = "TEST_KEY_";
        long current = System.currentTimeMillis();
//        final int [] contentLength = { 0 };
//        for (int i = 0; i < NB_LOOPS; i++) {
//          alias += i;
//          key += i;
//          buffer.put(key.getBytes());
//          buffer.flip();
//          
//          qdb.delete(session, alias);
//          qdb.put(session, alias, buffer, buffer.limit());
//          
//          buffer.clear();
//          qdb.get(session, alias, buffer, contentLength);
//          buffer.rewind();
//          
//          key = "TEST_KEY_UPDATE_"; 
//          buffer.put(key.getBytes());
//          qdb.update(session, alias, buffer, buffer.limit());
//          
//          //System.out.println(buffer.asCharBuffer());
//          
//          qdb.delete(session, alias);
//          alias = "alias_";
//          
//          System.out.println("QDB -> " + (System.currentTimeMillis() - current) );
//            current = System.currentTimeMillis();
//            
//            System.gc();
//        }   
        
        QuasardbConfig config = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode(QuasardbTest.HOST, QuasardbTest.PORT);
        config.addNode(node);
        Quasardb qdbDB = new Quasardb(config);

        for (int i = 0; i < 1000000; i++) {
            alias = "VALUE" + i;
            key += i;
            
            try {
                // Insert the provided value at provided key
                qdbDB.put(key, alias);
                System.out.println("QDB PUT " + i + " -> " + (System.currentTimeMillis() - current));
                current = System.currentTimeMillis();
                
                // Check if a value has been stored at key
                assertTrue(qdbDB.get(key).equals(alias));
                System.out.println("QDB GET " + i + " -> " + (System.currentTimeMillis() - current));
                current = System.currentTimeMillis();
                
                // Update value
                qdbDB.update(key, alias + "_UPDATED");
                System.out.println("QDB UPDATE " + i + " -> " + (System.currentTimeMillis() - current));
                current = System.currentTimeMillis();
                
                // Check if a value has been updated at key
                assertFalse(qdbDB.get(key).equals(alias));
                System.out.println("QDB GET " + i + " -> " + (System.currentTimeMillis() - current));
                current = System.currentTimeMillis();
                
                assertTrue(qdbDB.get(key).equals(alias + "_UPDATED"));
                System.out.println("QDB GET " + i + " -> " + (System.currentTimeMillis() - current));
                current = System.currentTimeMillis();
            } catch (QuasardbException e) {
                fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Clean up stored value
                try {
                    qdbDB.remove(key);
                    System.out.println("QDB DELETE " + i + " -> " + (System.currentTimeMillis() - current));
                    current = System.currentTimeMillis();
                } catch (QuasardbException e) {
                    e.printStackTrace();
                }
            } 
        }
        System.out.println("QDB 2 -> " + (System.currentTimeMillis() - current) );
        
        // Try to close qdb session
//        qdbError = qdb.close(session);
//        QuasardbManager.getInstance().getCache("TEST_QDB").close();
    }
}
