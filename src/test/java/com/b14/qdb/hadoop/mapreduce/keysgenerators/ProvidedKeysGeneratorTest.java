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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link ProvidedKeysGenerator} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    LibraryHelper.class,
    qdbJNI.class,
    qdb_error_t.class
})
public class ProvidedKeysGeneratorTest {
    private ProvidedKeysGenerator generator;
    
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

        BDDMockito.given(qdb_error_t.swigToEnum(any(int.class))).willReturn(qdb_error_t.error_ok);
        BDDMockito.given(qdbJNI.open()).willReturn(123L);
    }

    @After
    public void tearDown() {
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#ProvidedKeysGenerator()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testNoArgConstructorAndNoInitMeansIllegalState() throws Exception {
        generator = new ProvidedKeysGenerator();
        try {
            generator.getKeys(null);
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
        generator = new ProvidedKeysGenerator();
        try {
            generator.init(null);
            fail("Expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue("Exception should be a IllegalArgumentException", e instanceof IllegalArgumentException);
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#init(java.util.String)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testInitWithBadFormattedArgument() throws Exception {
        String initString = "key1_key2_key3";
        generator = new ProvidedKeysGenerator();
        generator.init(initString);
        assertTrue("InitString should be equals to provided init string.", generator.getInitString().equals(initString)); 
        assertTrue("Bad formatter keys list means empty set => " + generator.getKeys(new Quasardb()), generator.getKeys(new Quasardb()).isEmpty());
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#init(java.util.String)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testInit() throws Exception {
        String initString = "key1,key2,key3";
        generator = new ProvidedKeysGenerator();
        generator.init(initString);
        assertTrue("InitString should be equals to provided init string.", generator.getInitString().equals(initString));
        String storedKeys = new String();
        assertTrue("Stored keys [" + storedKeys + "] should be equals to init string => " + initString, generator.getKeys(new Quasardb()).containsAll(Arrays.asList(initString.split(","))));
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getKeys(com.b14.Quasardb)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeysWithUninitializedGeneratorMeansIllegalStateException() throws Exception {
        generator = new ProvidedKeysGenerator();
        try {
            generator.getKeys(new Quasardb());
            fail("Expected IllegalStateException");
        } catch (Exception e) {
            assertTrue("Exception should be a IllegalStateException", e instanceof IllegalStateException);
        }
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#getKeys(com.b14.Quasardb)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testGetKeys() throws Exception {
        Set<String> keys = new HashSet<String>();
        keys.add("key1");
        keys.add("key2");
        keys.add("key3");
        generator = new ProvidedKeysGenerator(keys);
        assertEquals("Keys should be equals.", generator.getKeys(new Quasardb()), keys);
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#equals(Object)}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testEquals() throws Exception {
        Set<String> keys = new HashSet<String>();
        keys.add("key1");
        keys.add("key2");
        keys.add("key3");
        generator = new ProvidedKeysGenerator(keys);
        assertTrue(!generator.equals(null));
        assertTrue(!generator.equals(new String("test")));
        
        ProvidedKeysGenerator generator2 = new ProvidedKeysGenerator(keys);
        assertEquals("Both generator should be equals.", generator, generator2);
        
        ProvidedKeysGenerator generator3 = new ProvidedKeysGenerator(Arrays.asList("Different_key"));
        assertTrue(!generator2.equals(generator3));
    }
    
    /**
     * Test method for {@link ProvidedKeysGenerator#hashCode()}.
     * 
     * @since 1.1.6
     */
    @Test
    public void testHashCode() {
        ProvidedKeysGenerator generator1 = new ProvidedKeysGenerator(Arrays.asList("key1","key2"));
        ProvidedKeysGenerator generator2 = new ProvidedKeysGenerator(Arrays.asList("key1","key2"));
        assertTrue(generator1.hashCode() == generator2.hashCode());
        
        ProvidedKeysGenerator generator3 = new ProvidedKeysGenerator(Arrays.asList("Different_key"));
        assertTrue(generator1.hashCode() != generator3.hashCode());
    }
}
