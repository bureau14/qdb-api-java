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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
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

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_remote_node_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link PrefixedKeysGenerator} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    LibraryHelper.class,
    qdbJNI.class,
    qdb_error_t.class,
    qdb.class,
    Quasardb.class
})
public class PrefixedKeysGeneratorTest {
    private static final String INIT_STRING = "prefix";
    private static final QuasardbConfig config = new QuasardbConfig();
    private static final List<String> expectedKeys = Arrays.asList(INIT_STRING + "_1", INIT_STRING + "_2", INIT_STRING + "_3", INIT_STRING + "_4", INIT_STRING + "_5");
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        PowerMockito.mockStatic(LibraryHelper.class);
        PowerMockito.mockStatic(qdbJNI.class);
        PowerMockito.mockStatic(qdb_error_t.class);
        PowerMockito.mockStatic(qdb.class);

        BDDMockito.given(qdb_error_t.swigToEnum(any(int.class))).willReturn(qdb_error_t.error_ok);
        BDDMockito.given(qdbJNI.open()).willReturn(123L);
        BDDMockito.given(qdbJNI.node_config(any(long.class), any(long.class), any(qdb_remote_node_t.class), any(long.class), any(error_carrier.class))).willReturn(ByteBuffer.wrap("Mock_Me".getBytes()));
    }

    @After
    public void tearDown() {
    }
    
    /**
     * Test method for {@link PrefixedKeysGenerator#PrefixedKeysGenerator(String)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testNoArgConstructorAndNoInitMeansIllegalState() throws Exception {
        Quasardb qdbInstance = PowerMockito.mock(Quasardb.class);
        when(qdbInstance.isConnected()).thenReturn(true);
        
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
        Quasardb qdbInstance = PowerMockito.mock(Quasardb.class);
        when(qdbInstance.isConnected()).thenReturn(true);
        when(qdbInstance.startsWith(any(String.class))).thenReturn(expectedKeys);
        PrefixedKeysGenerator generator = new PrefixedKeysGenerator();
        generator.init(INIT_STRING);
        Collection<String> keys = generator.getKeys(qdbInstance);
        assertTrue("Both list should be equals.", keys.containsAll(expectedKeys));
    }
}
