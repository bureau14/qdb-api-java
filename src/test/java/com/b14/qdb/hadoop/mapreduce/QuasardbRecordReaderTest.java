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
package com.b14.qdb.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link QuasardbRecordReader} class.
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
    Quasardb.class
})
public class QuasardbRecordReaderTest {
    private static final int PORT = 2836;
    private static final String HOST = "127.0.0.1";
    private static final QuasardbConfig config = new QuasardbConfig();
    private Quasardb qdbInstance = null;
    
    @Mock 
    private QuasardbInputSplit qdbInputSplit;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        QuasardbNode quasardbNode = new QuasardbNode(HOST, PORT);
        config.addNode(quasardbNode);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        
        PowerMockito.mockStatic(LibraryHelper.class);
        PowerMockito.mockStatic(qdbJNI.class);
        PowerMockito.mockStatic(qdb_error_t.class);
        PowerMockito.mockStatic(qdb.class);
        PowerMockito.mockStatic(Quasardb.class);
        
        BDDMockito.given(qdb_error_t.swigToEnum(any(int.class))).willReturn(qdb_error_t.error_ok);
        BDDMockito.given(qdbJNI.open()).willReturn(123L);
        BDDMockito.given(qdbJNI.connect(any(long.class), any(String.class), any(int.class))).willReturn(1);
        
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
            fail("qdbInstance not initialized.");
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (qdbInstance != null) {
                qdbInstance.purgeAll();
                qdbInstance.close();
            }
        } catch (QuasardbException e) {
        }
    }

    /**
     * Test method for {@link QuasardbRecordReader#getProgress()}.
     */
    @Test
    public void testGetProgressWithNoKeys() {
        when(qdbInputSplit.getQdbLocations()).thenReturn(new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836) });
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            qdbRecordReader.initialize(qdbInputSplit, null);
            assertTrue("No Keys means progress = 0.", 0 == qdbRecordReader.getProgress());
        } catch (Exception e) {
            fail("No Exception allowed.");
        }
    }

    /**
     * Test method for {@link QuasardbRecordReader#getProgress()}.
     */
    @Test
    public void testGetProgressWithNoKeysAndNoInitialization() {
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            assertTrue("No Keys means progress = 0.", 0 == qdbRecordReader.getProgress());
        } catch (Exception e) {
            fail("No Exception allowed.");
        }
    }

    /**
     * Test method for {@link QuasardbRecordReader#close()}.
     */
    @Test
    public void testCloseWithUnitializedRecordReader() {
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            qdbRecordReader.close();
            fail("Should raise an Exception because there is no connection available.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbRecordReader#close()}.
     */
    @Test
    public void testCloseAnAlreadyClosedRecordReader() {
        when(qdbInputSplit.getQdbLocations()).thenReturn(new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836) });
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            qdbRecordReader.initialize(qdbInputSplit, null);
            qdbRecordReader.close();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
        try {
            qdbRecordReader.close();
            fail("Should raise an Exception because RecordReader is already closed.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }        
    }
    
    /**
     * Test method for {@link QuasardbRecordReader#close()}.
     */
    @Test
    public void testClose() {
        when(qdbInputSplit.getQdbLocations()).thenReturn(new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836) });
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            qdbRecordReader.initialize(qdbInputSplit, null);
            qdbRecordReader.close();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
    }
    
    /**
     * Test method for {@link QuasardbRecordReader#getCurrentValue()}.
     */
    @Test
    public void testGetCurrentValueWithUninitializedRecordReader() {
        QuasardbRecordReader<String> qdbRecordReader = new QuasardbRecordReader<String>();
        try {
            assertTrue("Unitialized RecordReader means null values.", qdbRecordReader.getCurrentValue() == null);
        } catch (Exception e) {
            fail("No Exception allowed.");
        }
    }

    /**
     * Test method for {@link QuasardbRecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testInitializeWithNullParamMeansAnException() {
        try {
            new QuasardbRecordReader<String>().initialize(null, null);
            fail("Should raise an Exception because params are null");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbRecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testInitialize() {
        when(qdbInputSplit.getQdbLocations()).thenReturn(new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836) });
        try {
            new QuasardbRecordReader<String>().initialize(qdbInputSplit, null);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception because node is OK.");
        }
    }
}
