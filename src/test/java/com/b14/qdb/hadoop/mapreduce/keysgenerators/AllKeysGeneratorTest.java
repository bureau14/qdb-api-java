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
import java.util.Iterator;
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
import com.b14.qdb.QuasardbEntry;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_remote_node_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit tests case for {@link AllKeysGenerator} class.
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
public class AllKeysGeneratorTest {
    private static final List<String> expectedKeys = Arrays.asList("k1", "k2", "k3", "k4", "k5");
    
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
     * Test method for {@link AllKeysGenerator#getInitString()}.
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
     * Test method for {@link AllKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)}.
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
     * Test method for {@link AllKeysGenerator#getKeys(com.qdb.quasardb.Quasardb)}.
     * 
     * @since 1.1.6
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testGetKeys() throws Exception {
        Quasardb qdbInstance = PowerMockito.mock(Quasardb.class);
        when(qdbInstance.isConnected()).thenReturn(true);
        Iterator mockIterator = PowerMockito.mock(Iterator.class);
        when(qdbInstance.iterator()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, true, true, true, true, false);
        when(mockIterator.next()).thenReturn(new QuasardbEntry("k1", "v1"), new QuasardbEntry("k2", "v2"), new QuasardbEntry("k3", "v3"), new QuasardbEntry("k4", "v4"), new QuasardbEntry("k5", "v5"));
        AllKeysGenerator generator = new AllKeysGenerator();
        Collection<String> keys = generator.getKeys(qdbInstance);
        assertTrue("Both list should be equals.", keys.containsAll(expectedKeys));
    }
    
    /**
     * Test method for {@link AllKeysGenerator#equals(Object)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testEquals() {
        AllKeysGenerator generator1 = new AllKeysGenerator();
        generator1.init("init");
        assertTrue(!generator1.equals(null));
        assertTrue(!generator1.equals(new String("test")));
        
        AllKeysGenerator generator2 = new AllKeysGenerator();
        generator2.init("init");
        assertTrue(generator1.equals(generator2));
        
        AllKeysGenerator generator3 = new AllKeysGenerator();
        generator3.init("init3");
        assertTrue(!generator1.equals(generator3));
    }
    
    /**
     * Test method for {@link AllKeysGenerator#hashCode()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testHashCode() {
        AllKeysGenerator generator1 = new AllKeysGenerator("init");
        AllKeysGenerator generator2 = new AllKeysGenerator("init");
        assertTrue(generator1.hashCode() == generator2.hashCode());
        
        AllKeysGenerator generator3 = new AllKeysGenerator("different_init");
        assertTrue(generator1.hashCode() != generator3.hashCode());
    }
}
