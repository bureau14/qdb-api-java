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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.AllKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.IKeysGenerator;

/**
 * A container for configuration property names for jobs with quasardb input/output.
 * <br><br>
 * The job can be configured using the static methods in this class, {@link QuasardbInputFormat}, and {@link QuasardbOutputFormat}.
 * <br>
 * Alternatively, the properties can be set in the configuration with proper values.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbJobConf {
    public static final String CLUSTER_SIZE_PROPERTY = "hadoop.cluster.size";
    public static final String KEY_GENERATOR_CLASS = "keygenerator";
    public static final String KEY_GENERATOR_INIT_STRING = "keygenerator.init_string";
    public static final String QDB_NODES = "quasardb.nodes";
    
    private static final int MINIMUM_SPLIT_SIZE = 10;
    private static final String DEFAULT_QDB_NODE = "127.0.0.1:2836";
    private static final String QDB_NODES_SEPARATOR = ";";
    
    /**
     * Build a configuration for map/reduce job.
     * 
     * @since 1.3.0
     */
    public QuasardbJobConf() {
    }
    
    /**
     * Get the Hadoop cluster size property, provide a default in case it hasn't been set
     * 
     * @param conf the {@link Configuration} to get the property value from
     * @param defaultValue the default size to use if it hasn't been set
     * @return the hadoop cluster size or <code>defaultValue</code>
     * @since 1.3.0
     */
    public static int getHadoopClusterSize(Configuration conf, int defaultValue) {
        return conf.getInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, defaultValue);
    }
    
    /**
     * Set the Hadoop cluster size property, provide a default in case it hasn't been set
     * 
     * @param conf the {@link Configuration} to get the property value from
     * @param defaultValue the default size to use if it hasn't been set
     * @return the {@link Configuration} updated with the provided <code>hadoopClusterSize</code>
     * @since 1.3.0
     */
    public static Configuration setHadoopClusterSize(Configuration conf, int defaultValue) {
        conf.setInt(QuasardbJobConf.CLUSTER_SIZE_PROPERTY, defaultValue);
        return conf;
    }
    
    /**
     * Add a quasardb node to the {@link Configuration}.
     * 
     * @param conf the {@link Configuration} to add a location too
     * @param node the {@link QuasardbNode} to add
     * @return the {@link Configuration} with <code>location</code> added to the location property
     * @since 1.3.0
     */
    public static Configuration addLocation(Configuration conf, QuasardbNode node) {
        StringBuilder sb = new StringBuilder();
        String currentNodes = conf.get(QuasardbJobConf.QDB_NODES);
        
        if (currentNodes != null) {
            sb.append(currentNodes);
        } else {
            sb.append(DEFAULT_QDB_NODE);
        }
        if (sb.length() > 0) {
            sb.append(QuasardbJobConf.QDB_NODES_SEPARATOR);
        }
        sb.append(node.toString());

        conf.set(QuasardbJobConf.QDB_NODES, sb.toString());
        return conf;
    }
    
    /**
     * Get all the quasardb nodes from the provided {@link Configuration}
     * 
     * @param conf the {@link Configuration}
     * @return an array of {@link QuasardbNode} (may be empty, never null)
     * @throws IOException if provided configuration is wrong (bad hostname or port) or null
     * @since 1.3.0
     */
    public static QuasardbNode[] getNodesLocation(Configuration conf) throws IOException {
        if (conf == null) {
            throw new IllegalStateException("No configuration provided");
        }
        final List<QuasardbNode> result = new ArrayList<QuasardbNode>();
        
        final String nodes = conf.get(QuasardbJobConf.QDB_NODES, QuasardbJobConf.DEFAULT_QDB_NODE);
        final StringTokenizer st = new StringTokenizer(nodes, QuasardbJobConf.QDB_NODES_SEPARATOR);
        try {
            while (st.hasMoreTokens()) {
                String node = st.nextToken();
                result.add(new QuasardbNode(node.substring(0, node.indexOf(':')), Integer.parseInt(node.substring(node.indexOf(':') + 1, node.length()))));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Wrong provided input format => " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new IOException("Wrong port format for provided input => " + e.getMessage());
        }
        return result.toArray(new QuasardbNode[result.size()]);
    }
    
    /**
     * Retrieve keys provider from the passed {@link Configuration}
     * 
     * @param conf the {@link Configuration} to query
     * @return the {@link IKeysGenerator} the job was configured with
     * @throws IOException from a call to {@link IKeysGenerator#init(String)}
     * @throws RuntimeException if a {@link IllegalAccessException} or {@link InstantiationException} is thrown creating a {@link IKeysGenerator}
     * @since 1.3.0
     */
    public static IKeysGenerator getKeysGeneratorFromConf(Configuration conf) throws IOException {
        if (conf == null) {
            throw new IllegalStateException("No configuration provided");
        }
        Class<? extends IKeysGenerator> clazz = conf.getClass(QuasardbJobConf.KEY_GENERATOR_CLASS, AllKeysGenerator.class, IKeysGenerator.class);
        try {
            IKeysGenerator generator = clazz.newInstance();
            generator.init(conf.get(QuasardbJobConf.KEY_GENERATOR_INIT_STRING));
            return generator;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access to provided keys generator class => " + e.getMessage());
        } catch (InstantiationException e) {
            throw new RuntimeException("Cannot initialize provided keys generator class => " + e.getMessage());
        }
    }
    
    /**
     * Set the {@link IKeysGenerator} implementation to use.
     * 
     * @param conf the {@link Configuration} to update
     * @param keyGenerator the {@link IKeysGenerator} to use
     * @return the configuration updated with a serialized version of the lister provided
     * @since 1.3.0
     */
    public static <T extends IKeysGenerator> Configuration setKeysGenerator(Configuration conf, T keyGenerator) throws IOException {
        if (conf == null) {
            throw new IllegalStateException("No configuration provided");
        }
        conf.setClass(KEY_GENERATOR_CLASS, keyGenerator.getClass(), IKeysGenerator.class);
        conf.setStrings(KEY_GENERATOR_INIT_STRING, keyGenerator.getInitString());
        return conf;
    }
    
    /**
     * Retrieve minimum split size
     * 
     * @return minimum split size
     * @since 1.3.0
     */
    public static int getMinSplitSize() {
        return QuasardbJobConf.MINIMUM_SPLIT_SIZE;
    }
}
