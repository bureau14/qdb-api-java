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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;

/**
 * A unit test case for {@link QuasardbRecordWriter} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbRecordWriterIT {
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
    public void testQuasardbRecordWriterWithWrongNodeMeansAnException() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2837");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        try {
            new QuasardbRecordWriter<String>(taskAttemptContext);
            fail("Should raise an Exception because provided node is unreachable.");
        } catch (Exception e) {
            assertTrue("Should raise an QuasardbException", e instanceof QuasardbException);
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
    
    /**
     * Test method for {@link QuasardbRecordWriter#write(Text, Object)}.
     */
    @Test
    public void testWrite() {
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        QuasardbRecordWriter<String> qdbRecordWriter = null;
        try {
            qdbRecordWriter = new QuasardbRecordWriter<String>(taskAttemptContext);
            qdbRecordWriter.write(new Text("test_record_writer_key"), "test_record_writer_value");
            
            QuasardbConfig config = new QuasardbConfig();
            QuasardbNode quasardbNode = new QuasardbNode("127.0.0.1", 2836);
            config.addNode(quasardbNode);
            Quasardb qdbInstance = new Quasardb(config);
            qdbInstance.connect();
            assertTrue("A value should be stored at key [test_record_writer_key].", "test_record_writer_value".equals(qdbInstance.get("test_record_writer_key")));
            
            qdbInstance.purgeAll();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
        try {
            qdbRecordWriter.close(taskAttemptContext);
        } catch (Exception e) {}
    }
}
