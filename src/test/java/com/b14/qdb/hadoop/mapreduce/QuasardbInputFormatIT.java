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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.b14.qdb.Qdb;
import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.data.Pojo;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.IKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.PrefixedKeysGenerator;

/**
 * A unit test case for {@link QuasardbInputFormat} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbInputFormatIT {
    private static final int PORT = 2836;
    private static final String HOST = "127.0.0.1";
    private static final QuasardbConfig config = new QuasardbConfig();
    
    private QuasardbInputFormat<?> inputFormat;
    private Quasardb qdbInstance = null;
    
    @Mock 
    public JobContext jobContext;
    
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
        MockitoAnnotations.initMocks(this);
        inputFormat = new QuasardbInputFormat<String>();
        
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
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
     * Test method for {@link com.qdb.quasardb.hadoop.mapreduce.QuasardbInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}.
     */
    @Test 
    public void getSplitsWithDefaultValues() throws Exception {
        qdbInstance.purgeAll();
        try {
            Thread.sleep(500);
        } catch (Exception e) {}
        for (int i = 0; i < 100; i++) {
            qdbInstance.put("key_" + i, "value_" + i);
        }
        
        Configuration conf = new Configuration();
        when(jobContext.getConfiguration()).thenReturn(conf);
        
        int totalLength = 0;
        List<InputSplit> splits = inputFormat.getSplits(jobContext);
        assertEquals("There should be 4 splits.", 4, splits.size());
        
        for (InputSplit split : splits) {
            totalLength += split.getLength();
        }
        assertEquals("Total keys should be equals to 100.", 100, totalLength);
    }
    
    /**
     * Test method for {@link com.qdb.quasardb.hadoop.mapreduce.QuasardbInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}.
     */
    @Test 
    public void getSplits() throws Exception {
        qdbInstance.purgeAll();
        try {
            Thread.sleep(500);
        } catch (Exception e) {}
        for (int i = 0; i < 100001; i++) {
            Pojo pojo = new Pojo();
            pojo.setText("value_" + i);
            pojo.setAbc(i);
            Pojo child = new Pojo();
            child.setText("child_value_" + i);
            pojo.setChild(child);
            qdbInstance.put("key_" + i, pojo);
        }
        
        Configuration conf = new Configuration();
        conf.setInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, 10);
        conf.setClass(QuasardbJobConf.KEY_GENERATOR_CLASS, PrefixedKeysGenerator.class, IKeysGenerator.class);
        conf.set(QuasardbJobConf.KEY_GENERATOR_INIT_STRING, "key_");
        when(jobContext.getConfiguration()).thenReturn(conf);
        
        int totalLength = 0;
        List<InputSplit> splits = inputFormat.getSplits(jobContext);
        assertEquals("There should be 101 splits.", 101, splits.size());
        for (InputSplit split : splits) {
            totalLength += split.getLength();
        }
        assertEquals("Total keys should be equals to 100001.", 100001, totalLength);
    }
    
    /**
     * Test method for {@link com.qdb.quasardb.hadoop.mapreduce.QuasardbInputFormat#getSplitSize(int, int)}.
     */
    @Test 
    public void getSplitSize() {
        assertEquals(10, inputFormat.getSplitSize(10, 4));
        assertEquals(20, inputFormat.getSplitSize(800, 4));
        assertEquals(2500, inputFormat.getSplitSize(100000, 4));
    }
}
