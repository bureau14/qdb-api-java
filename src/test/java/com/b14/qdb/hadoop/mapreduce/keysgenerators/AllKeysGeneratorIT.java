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
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
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
package com.b14.qdb.hadoop.mapreduce.keysgenerators;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;

/**
 * A unit tests case for {@link AllKeysGenerator} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class AllKeysGeneratorIT {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 2836;
    private static final QuasardbConfig config = new QuasardbConfig();
    private static final List<String> expectedKeys = Arrays.asList("k1", "k2", "k3", "k4", "k5");
    private Quasardb qdbInstance = null;
    
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
            for (String key : expectedKeys) {
                qdbInstance.put(key, "value_for_" + key);    
            }
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("qdbInstance not initialized.");
        }
    }

    @After
    public void tearDown() {
        try {
            if (qdbInstance != null) {
                qdbInstance.purgeAll();
                qdbInstance.close();
            }
        } catch (QuasardbException e) {
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getInitString()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetInitString() throws Exception {
        String testString = new String("TEST");
        AllKeysGenerator generator = new AllKeysGenerator();
        assertFalse("Should not be equals to " + testString + " because generator wasn't initialized.", testString.equals(generator.getInitString()));
        assertTrue("Should be equals to null because generator wasn't initialized.", generator.getInitString() == null);
        generator.init(testString);
        assertTrue("Should be equals to " + testString + " because generator was initialized.", testString.equals(generator.getInitString()));
    }
    
    /**
     * Test method for {@link PrefixedKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeysNoArgMeansIllegalState() throws Exception {
        AllKeysGenerator generator = new AllKeysGenerator();
        try {
            generator.getKeys(null);
            fail("Expected IllegalStateException");
        } catch (Exception e) {
            assertTrue("Exception should be a IllegalStateException", e instanceof IllegalStateException);
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeysWithoutExplicitConnection() throws Exception {
        AllKeysGenerator generator = new AllKeysGenerator();
        Collection<String> keys = generator.getKeys(new Quasardb(config));
        assertTrue("Both list should be equals.", keys.containsAll(expectedKeys));
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeys() throws Exception {
        AllKeysGenerator generator = new AllKeysGenerator();
        Collection<String> keys = generator.getKeys(qdbInstance);
        assertTrue("Both list should be equals.", keys.containsAll(expectedKeys));
    }
}
