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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.b14.qdb.QuasardbNode;

/**
 * A unit test case for {@link QuasardbOutputFormat} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbOutputFormatIT {
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
     * Test method for {@link QuasardbOutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)}.
     */
    @Test
    public void testCheckOutputSpecs() {
        // Nothing to test
    }

    /**
     * Test method for {@link QuasardbOutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testGetOutputCommitter() {
        QuasardbOutputFormat<String> qdbOutputFormat = new QuasardbOutputFormat<String>();
        try {
            assertTrue("OutputCommitter shouldn't be null.", qdbOutputFormat.getOutputCommitter(taskAttemptContext) != null);
            assertTrue("Should be an instance of OutputCommitter", OutputCommitter.class.isInstance(qdbOutputFormat.getOutputCommitter(taskAttemptContext)));
        } catch (Exception e) {
            fail("No Exception allowed.");
        }
    }

    /**
     * Test method for {@link QuasardbOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testGetRecordWriter() {
        QuasardbOutputFormat<String> qdbOutputFormat = new QuasardbOutputFormat<String>();
        Configuration conf = new Configuration();
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode("127.0.0.1", 2836));
        when(taskAttemptContext.getConfiguration()).thenReturn(conf);
        try {
            assertTrue("RecordWriter shouldn't be null.", qdbOutputFormat.getRecordWriter(taskAttemptContext) != null);
            assertTrue("Should be an instance of OutputCommitter", OutputCommitter.class.isInstance(qdbOutputFormat.getOutputCommitter(taskAttemptContext)));
        } catch (Exception e) {
            fail("No Exception allowed.");
        }
    }

}
