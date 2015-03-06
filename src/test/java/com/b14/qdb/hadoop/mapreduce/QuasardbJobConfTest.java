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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.IKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.ProvidedKeysGenerator;

/**
 * A unit test case for {@link QuasardbJobConf} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbJobConfTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getHadoopClusterSize(org.apache.hadoop.conf.Configuration, int)}.
     */
    @Test
    public void testGetHadoopClusterSizeWithNoPreviousValue() {
        int clusterSize = QuasardbJobConf.getHadoopClusterSize(new Configuration(), 1);
        assertTrue("Cluster size should be equals to 1.", clusterSize == 1);
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getHadoopClusterSize(org.apache.hadoop.conf.Configuration, int)}.
     */
    @Test
    public void testGetHadoopClusterSizeWithPreviousValue() {
        Configuration conf = new Configuration();
        conf.setInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, 10);
        int clusterSize = QuasardbJobConf.getHadoopClusterSize(conf, 1);
        assertTrue("Cluster size should be equals to 1.", clusterSize == 10);
    }
    
    /**
     * Test method for {@link QuasardbJobConf#setHadoopClusterSize(org.apache.hadoop.conf.Configuration, int)}.
     */
    @Test
    public void testSetHadoopClusterSizeWithPreviousValue() {
        Configuration conf = new Configuration();
        conf.setInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, 10);
        assertTrue("Cluster size should be equals to setted value => 1.", QuasardbJobConf.setHadoopClusterSize(conf, 1).getInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, 10000) == 1);
    }
    
    /**
     * Test method for {@link QuasardbJobConf#addLocation(org.apache.hadoop.conf.Configuration, QuasardbNode)}.
     */
    @Test
    public void testAddLocationWithNoPreviousLocations() {
        final String host = "127.0.0.1";
        final int port = 2837;
        Configuration conf = new Configuration();
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode(host, port));
        assertEquals("Both config should be equals.", "127.0.0.1:2836;" + host + ":" + port, conf.get(QuasardbJobConf.QDB_NODES));
    }
    
    /**
     * Test method for {@link QuasardbJobConf#addLocation(org.apache.hadoop.conf.Configuration, QuasardbNode)}.
     */
    @Test
    public void testAddLocationWithPreviousLocations() {
        final String host = "127.0.0.1";
        final int port = 2836;
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, host + ":" + port);
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode(host, port));
        assertEquals("Both config should be equals.", host + ":" + port + ";" + host + ":" + port, conf.get(QuasardbJobConf.QDB_NODES));
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getNodesLocation(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetNodesLocationWithNullParamMeansAnException() {
        try {
            QuasardbJobConf.getNodesLocation(null);
            fail("A null param should raise an Exception");
        } catch (Exception e) {
            assertTrue("Should be an instance of IllegalStateException", e instanceof IllegalStateException);
        }
    }

    /**
     * Test method for {@link QuasardbJobConf#getNodesLocation(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetNodesLocationWithWrongParam() {
        try {
            final String host = "127.0.0.1";
            final int port = 2836;
            Configuration conf = new Configuration();
            conf.set(QuasardbJobConf.QDB_NODES, host + "_" + port);
            QuasardbJobConf.getNodesLocation(conf);
            fail("A null param should raise an Exception");
        } catch (Exception e) {
            assertTrue("Should be an instance of IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getNodesLocation(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetNodesLocationWithOneLocation() {
        try {
            final String host = "127.0.0.1";
            final int port = 2836;
            Configuration conf = new Configuration();
            conf.set(QuasardbJobConf.QDB_NODES, host + ":" + port);
            QuasardbNode[] nodes = QuasardbJobConf.getNodesLocation(conf);
            assertTrue("Nodes list should contain only one element", nodes.length == 1);
            assertTrue("First node port should be equals to " + port, nodes[0].getPort() == port);
            assertTrue("First node hostname should be equals to " + host, nodes[0].getHostName().equals(host));
        } catch (IOException e) {
            fail("Shouldn't raise an Exception.");
        }
    }

    /**
     * Test method for {@link QuasardbJobConf#getNodesLocation(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetNodesLocationWithTwoLocation() {
        try {
            final String host1 = "127.0.0.1";
            final int port1 = 2836;
            final String host2 = "localhost";
            final int port2 = 2837;
            List<QuasardbNode> expectedNodes = Arrays.asList(new QuasardbNode(host1, port1), new QuasardbNode(host2, port2));
            Configuration conf = new Configuration();
            conf.set(QuasardbJobConf.QDB_NODES, host1 + ":" + port1 + ";" + host2 + ":" + port2);
            QuasardbNode[] nodes = QuasardbJobConf.getNodesLocation(conf);
            assertTrue("Nodes list should contain only one element", nodes.length == 2);
            assertTrue("Nodes list should be equals to " + expectedNodes, Arrays.asList(nodes).toString().equals(expectedNodes.toString()));
        } catch (IOException e) {
            fail("Shouldn't raise an Exception.");
        }
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getKeysGeneratorFromConf(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetKeysGeneratorFromConfWithNullParamMeansAnException() {
        try {
            QuasardbJobConf.getKeysGeneratorFromConf(null);
            fail("A null param should raise an Exception");
        } catch (Exception e) {
            assertTrue("Should be an instance of IllegalStateException", e instanceof IllegalStateException);
        }
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getKeysGeneratorFromConf(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetKeysGeneratorFromConfWithNoInitStringMeansAnException() {
        try {
            Configuration conf = new Configuration();
            conf.setClass(QuasardbJobConf.KEY_GENERATOR_CLASS, ProvidedKeysGenerator.class, IKeysGenerator.class);
            QuasardbJobConf.getKeysGeneratorFromConf(conf);
            fail("Should raise an Exception because no init string was provided with ProvidedKeysGenerator.");
        } catch (Exception e) {
            assertTrue("Should be an instance of IllegalStateException", e instanceof IllegalArgumentException);
        }
    }
    
    /**
     * Test method for {@link QuasardbJobConf#getKeysGeneratorFromConf(org.apache.hadoop.conf.Configuration)}.
     */
    @Test
    public void testGetKeysGeneratorFromConf() {
        try {
            final Set<String> keys = new HashSet<String>();
            keys.add("key1");
            keys.add("key2");
            keys.add("key3");
            Configuration conf = new Configuration();
            conf.setClass(QuasardbJobConf.KEY_GENERATOR_CLASS, ProvidedKeysGenerator.class, IKeysGenerator.class);
            conf.set(QuasardbJobConf.KEY_GENERATOR_INIT_STRING, "key1,key2,key3");
            ProvidedKeysGenerator generator = new ProvidedKeysGenerator(keys); 
            assertEquals("Configured generator should be equals to " + generator, generator, QuasardbJobConf.getKeysGeneratorFromConf(conf));
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
    }

    /**
     * Test method for {@link QuasardbJobConf#getMinSplitSize()}.
     */
    @Test
    public void testGetMinSplitSize() {
        assertTrue("Minimum split size should be equals to 10.", QuasardbJobConf.getMinSplitSize() == 10);
    }
}
