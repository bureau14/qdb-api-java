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

import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.databene.benerator.anno.Generator;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.feed4junit.Feeder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A integration parallel tests case for {@link Quasardb} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.6
 */
@RunWith(Feeder.class)
public class QuasardbParallelIT {
    private static final int NB_LOOPS = 10;
    private static final int NB_THREADS = 5;
    private static final int REQ_AVERAGE_EXECUTION_TIME = 5000;
    private static final int REQ_MAX_LATENCY = 20000;
    private static final int REQ_MEDIAN_LATENCY = 5000;
    private static final int REQ_THROUGHPUT = 1;
    private static final String GENERATOR_NAME = "com.b14.qdb.data.ParallelDataGenerator";
    private static final QuasardbConfig config = new QuasardbConfig();
    private static final Quasardb qdb = new Quasardb();
    private final AtomicInteger counter = new AtomicInteger();
    
    @Rule 
    public ContiPerfRule rule = new ContiPerfRule(new HtmlReportModule());
   
    public QuasardbParallelIT() {
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Qdb.DAEMON.start();
        QuasardbNode node = new QuasardbNode(QuasardbIT.HOST, QuasardbIT.PORT);
        config.addNode(node);
        try {
            qdb.setConfig(config); 
            qdb.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("Could not initialize Quasardb connection pool for Loader => " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception allowed in test class setup => " + e.getMessage());
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
        Qdb.DAEMON.stop();
    }
    
    @Before
    public void init() throws Exception {
    }
    
    @After
    public void cleanUp() {
    }
    
    @Test
    @Generator(GENERATOR_NAME)
    @PerfTest(invocations = NB_LOOPS, 
              threads = NB_THREADS)
    @Required(average = REQ_AVERAGE_EXECUTION_TIME, 
              max = REQ_MAX_LATENCY, 
              median = REQ_MEDIAN_LATENCY, 
              throughput = REQ_THROUGHPUT)
    public void quasardbParallelTest(String key, Object value) {
        synchronized(this) {
            key += "_" + this.counter.incrementAndGet();
        }
        
        // Try to clean up stored value if exists
        try {
            if (qdb != null) {
                qdb.remove(key);
            }
        } catch (QuasardbException e) {
            // Nothing to do
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception allowed here => " + e.getMessage());
        }
        
        try {
            if (qdb != null) {
                // Insert the provided value at provided key
                qdb.put(key , value);
                
                // Check if a value has been stored at key
                if (!qdb.get(key).equals(value)) {
                    fail("Stored value at key [" + key + "] should be equals to " + value);
                }
                
                // Update value
                qdb.update(key, value + "_UPDATED");
                
                // Check if a value has been updated at key
                if (qdb.get(key).equals(value)) {
                    fail("Stored value at key [" + key + "] shouldn't be equals to " + value);
                }
                if (!qdb.get(key).equals(value + "_UPDATED")) {
                    fail("Stored value at key [" + key + "] should be equals to " + value + "_UPDATED");
                }
                
                // Get and replace
                if (!qdb.getAndUpdate(key, value).equals(value + "_UPDATED")) {
                    fail("Previous stored value at key [" + key + "] shouldn't be equals to " + value + "_UPDATED");
                }
                if (!qdb.get(key).equals(value)) {
                    fail("New stored value at key [" + key + "] shouldn't be equals to " + value);
                }
                
                // Get and remove
                if (!qdb.getAndRemove(key).equals(value)) {
                    fail("Previous stored value at key [" + key + "] shouldn't be equals to " + value);
                }
                try {
                    qdb.get(key);
                    fail("Key " + key + " should have been removed by getAndRemove().");
                } catch (Exception e) {
                    if (! (e instanceof QuasardbException)) {
                        fail("Should raise a QuasardbException => " + e.toString());
                    }
                }
                
                // Remove if
                qdb.put(key , value);
                if (qdb.removeIf(key, value + "_FAUX")) {
                    fail("Value " + value + "_FAUX for key " + key + " doesn't match content => removeIf should return false");
                }
                if (!qdb.removeIf(key, value)) {
                    fail("Key [" + key + "] should have been removed successfully");
                }
                try {
                    qdb.get(key);
                    fail("Should raise an Exception because key " + key + " should have been removed by previous removeIf().");
                } catch (Exception e) {
                    if (! (e instanceof QuasardbException)) {
                        fail("Should raise a QuasardbException => " + e.toString());
                    }
                }
            } else {
                fail("qdb object shouldn't be null.");
            }
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("Cannot insert or read key[" + key + "] ->" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception => " + e.toString());
        } finally {
            // Clean up stored value
            try {
                if (qdb != null) {
                    qdb.remove(key);
                }
            } catch (QuasardbException e) {
            }
        } 
    }
}
 
