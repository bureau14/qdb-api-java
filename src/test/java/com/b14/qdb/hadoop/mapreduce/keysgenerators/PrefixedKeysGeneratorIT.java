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

import com.b14.qdb.Qdb;
import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;

/**
 * A unit test case for {@link PrefixedKeysGenerator} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class PrefixedKeysGeneratorIT {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 2836;
    private static final String INIT_STRING = "prefix";
    private static final QuasardbConfig config = new QuasardbConfig();
    private static final List<String> expectedKeys = Arrays.asList(INIT_STRING + "_1", INIT_STRING + "_2", INIT_STRING + "_3", INIT_STRING + "_4", INIT_STRING + "_5");
    private Quasardb qdbInstance = null;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Qdb.DAEMON.start();
        QuasardbNode quasardbNode = new QuasardbNode(HOST, PORT);
        config.addNode(quasardbNode);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Qdb.DAEMON.stop();
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
     * Test method for {@link PrefixedKeysGenerator#PrefixedKeysGenerator(String)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testNoArgConstructorAndNoInitMeansIllegalState() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator(null);
        try {
            generator.getKeys(qdbInstance);
            fail("Expected IllegalStateException");
        } catch (Exception e) {
            assertTrue("Exception should be a IllegalStateException", e instanceof IllegalStateException);
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#init(java.util.String)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testInitWithNoArgMeansIllegalArgument() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
        try {
            generator.init(null);
            fail("Expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue("Exception should be a IllegalArgumentException", e instanceof IllegalArgumentException);
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getInitString()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetInitString() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
        assertFalse("Should not be equals to " + INIT_STRING + " because generator wasn't initialized.", INIT_STRING.equals(generator.getInitString()));
        assertTrue("Should be equals to null because generator wasn't initialized.", generator.getInitString() == null);
        generator.init(INIT_STRING);
        assertTrue("Should be equals to " + INIT_STRING + " because generator was initialized.", INIT_STRING.equals(generator.getInitString()));
    }
    
    /**
     * Test method for {@link PrefixedKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeysNoArgMeansIllegalState() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
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
    public void testGetKeysWithoutConnection() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
        try {
            generator.getKeys(new Quasardb(config));
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
    public void testGetKeys() throws Exception {
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
        generator.init(INIT_STRING);
        Collection<String> keys = generator.getKeys(qdbInstance);
        assertTrue("Both list should be equals.", keys.containsAll(expectedKeys));
    }
}
