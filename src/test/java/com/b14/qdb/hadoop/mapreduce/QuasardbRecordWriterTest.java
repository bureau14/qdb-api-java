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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link QuasardbRecordWriter} class.
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
public class QuasardbRecordWriterTest {
    @Mock 
    private TaskAttemptContext taskAttemptContext;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
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
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link QuasardbRecordWriter#QuasardbRecordWriter(TaskAttemptContext)}.
     */
    @Test
    public void testQuasardbRecordWriter() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        try {
            new QuasardbRecordWriter<String>(taskAttemptContext);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception because provided node is OK.");
        }
    }

    /**
     * Test method for {@link QuasardbRecordWriter#QuasardbRecordWriter(TaskAttemptContext)}.
     */
    @Test
    public void testQuasardbRecordWriterWithWrongNodePortMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:foo");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        try {
            new QuasardbRecordWriter<String>(taskAttemptContext);
            fail("Should raise an Exception because provided node is unreachable.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbRecordWriter#close(TaskAttemptContext)}.
     */
    @Test
    public void testClose() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        try {
            QuasardbRecordWriter<String> qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception because provided node is OK.");
        } 
    }
    
    /**
     * Test method for {@link QuasardbRecordWriter#close(TaskAttemptContext)}.
     */
    @Test
    public void testCloseAnAlreadyClosedRecordWriterMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        QuasardbRecordWriter<String> qdbRecordWriter = null;
        try {
            qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception because provided node is OK.");
        }
        
        try {
            qdbRecordWriter.close(taskAttemptContext);
            fail("Should raise an Exception because recordwriter is already closed.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
    }

    /**
     * Test method for {@link QuasardbRecordWriter#write(Text, Object)}.
     */
    @Test
    public void testWriteNullKeyMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        QuasardbRecordWriter<String> qdbRecordWriter = null;
        try {
            qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.write(null, "test");
            fail("Should raise an Exception because provided key is null.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
        try {
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {}
    }
    
    /**
     * Test method for {@link QuasardbRecordWriter#write(Text, Object)}.
     */
    @Test
    public void testWriteNullValueMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        QuasardbRecordWriter<String> qdbRecordWriter = null;
        try {
            qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.write(new Text("test"), null);
            fail("Should raise an Exception because provided value is null.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
        try {
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {}
    }

    /**
     * Test method for {@link QuasardbRecordWriter#write(Text, Object)}.
     */
    @Test
    public void testWriteAReservedKeyMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        QuasardbRecordWriter<String> qdbRecordWriter = null;
        try {
            qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.write(new Text("qdb.reserved"), "test");
            fail("Should raise an Exception because provided value is null.");
        } catch (Exception e) {
            assertTrue("Should raise an IOException", e instanceof IOException);
        }
        try {
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {}
    }
}
