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

package net.quasardb.qdb;

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

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.tools.LibraryHelper;

/**
 * A integration RAW tests case for {@link Quasardb} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version 2.0.0
 * @since 1.1.6
 */
@RunWith(Feeder.class)
public class QdbRawTest {
    private static final int NB_LOOPS = 1;
    private static final int NB_THREADS = 10;
    private static final int REQ_AVERAGE_EXECUTION_TIME = 1000;
    private static final String GENERATOR_NAME = "net.quasardb.qdb.data.ParallelDataGenerator";
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
    
    private static SWIGTYPE_p_qdb_session session;
    private QdbCluster cluster;
    
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
        Qdb.DAEMON.start();
        
        // **** INIT RAW API ****
        // Try to opening a qdb session
        session = qdb.open();
        
        // Try to connect to the qdb node
        qdb_error_t qdbError = qdb.connect(session, "qdb://127.0.0.1:2836");
        
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            System.out.println(qdbError);
            System.exit(0);
        }
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        // **** CLOSE RAW API ****
        qdb_error_t qdbError = qdb.close(session);
        if (qdbError != qdb_error_t.error_ok) {
            System.out.println(qdbError);
            System.exit(0);
        }
        
        Qdb.DAEMON.stop();
    }
    
    @Before
    public void init() {
        try {
            cluster = new QdbCluster("qdb://127.0.0.1:2836");
        } catch (Exception e) {
            System.err.println("Could not initialize Quasardb connection pool for Loader: " + e.toString());
            e.printStackTrace();
            return;
        }
    }
    
    @After
    public void cleanUp() {
        try {
            if (cluster != null) {
                cluster.disconnect();
            }
        } catch (QdbException e) {
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
            
            bufferGet = qdb.get(session, key, error);
            
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
            cluster.removeEntry(key);
        } catch (QdbException e) {
        }
        
        try {
            // Insert the provided value at provided key
            java.nio.ByteBuffer content = java.nio.ByteBuffer.allocateDirect(((String) value).getBytes().length);
            content.put(((String) value).getBytes());
            content.flip();
            cluster.getBlob(key).put(content);
            
            // Check if a value has been stored at key
            java.nio.ByteBuffer buffer = cluster.getBlob(key).get();
            byte[] bytes = new byte[buffer.limit()];
            buffer.rewind();
            buffer.get(bytes, 0, buffer.limit());
            assertTrue(new String(bytes).equals(value));
            
            // Update value
            content = java.nio.ByteBuffer.allocateDirect(((String) value + "_UPDATED").getBytes().length);
            content.put(((String) value + "_UPDATED").getBytes());
            content.flip();
            cluster.getBlob(key).update(content);
            
            // Check if a value has been updated at key
            buffer = cluster.getBlob(key).get();
            bytes = new byte[buffer.limit()];
            buffer.rewind();
            buffer.get(bytes, 0, buffer.limit());
            assertFalse(new String(bytes).equals(value));
            assertTrue(new String(bytes).equals(value + "_UPDATED"));
        } catch (QdbException e) {
            e.printStackTrace();
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
        } finally {
            // Clean up stored value
            try {
                if (cluster != null) {
                    cluster.removeEntry(key);
                }
            } catch (QdbException e) {
            }
        } 
    }
}
