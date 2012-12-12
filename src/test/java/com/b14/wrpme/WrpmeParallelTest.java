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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
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
public class QuasardbParallelTest {
    private static final String WRPME_NAME = "testParallel";
    private static final int NB_LOOPS = 2;
    private static final int NB_THREADS = 10;
    private static final int REQ_AVERAGE_EXECUTION_TIME = 1000;
    private static final String GENERATOR_NAME = "com.b14.qdb.data.ParallelDataGenerator";
    private static final Map<String,String> config = new HashMap<String,String>();
    private static final Quasardb qdb = new Quasardb();
    
    @Rule
    public ContiPerfRule i = new ContiPerfRule(new CustomContiperfFileExecutionLogger());
    
    public QuasardbParallelTest() {
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
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME)
    public void putGetUpdateDeleteTest(String key, Object value) {
        key += "_" + Thread.currentThread().getId();
        
        // Try to clean up stored value if exists
        try {
            qdb.remove(key);
        } catch (QuasardbException e) {
        }
        
        try {
            // Insert the provided value at provided key
            qdb.put(key , value);
            
            // Check if a value has been stored at key
            assertTrue(qdb.get(key).equals(value));
            
            // Update value
            qdb.update(key, value + "_UPDATED");
            
            // Check if a value has been updated at key
            assertFalse(qdb.get(key).equals(value));
            assertTrue(qdb.get(key).equals(value + "_UPDATED"));
        } catch (QuasardbException e) {
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        } 
    }
}
