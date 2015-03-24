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
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.IKeysGenerator;

/**
 * quasardb specific {@link InputFormat} for Hadoop Map/Reduce.
 * 
 * @param <V> value type
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see InputFormat
 * @version master
 * @since 1.3.0
 */
public class QuasardbInputFormat<V> extends InputFormat<Text, V> {
    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public RecordReader<Text, V> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new QuasardbRecordReader<V>();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        // Retrieve nodes if defined (they work as entry point to the cluster)
        QuasardbNode[] nodes = QuasardbJobConf.getNodesLocation(conf);
        if (nodes.length == 0) {
            throw new IOException("No nodes provided");
        }
        
        // Retrieve keys using a generator if defined
        final IKeysGenerator generator = QuasardbJobConf.getKeysGeneratorFromConf(conf);
        final QuasardbConfig qdbConf = new QuasardbConfig();
        for (QuasardbNode node : nodes) {
            qdbConf.addNode(node);
        }
        List<String> keys = new ArrayList<String>();
        Quasardb qdb = new Quasardb(qdbConf);
        keys.addAll(generator.getKeys(qdb));
        
        // Create splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        
        // Connect to quasardb instance if key generator didn't
        try {
            if (!qdb.isConnected()) {
                qdb.connect();
            }
        } catch (QuasardbException e) {
            throw new IOException(e);
        }
        
        // Browse each provided keys to find their location on cluster
        HashMap<QuasardbNode, List<String>> nodesMap = new HashMap<QuasardbNode, List<String>>();
        QuasardbNode node = null;
        for (String key : keys) {
            // Search key location for data affinity
            try {
                node = qdb.getLocation(key);
            } catch (QuasardbException e) {
                // set default location
                node = nodes[0];
            }

            // Gather keys by nodes
            List<String> keysOnNodes = nodesMap.get(node);
            if (keysOnNodes == null) {
                keysOnNodes = new ArrayList<String>();
                keysOnNodes.add(key);
                nodesMap.put(node, keysOnNodes);
            } else {
                keysOnNodes.add(key);
            }
        }
        
        // Compute nb splits
        for (QuasardbNode qdbNode : nodesMap.keySet()) {
            int startIndex = 0;
            int numberOfKeys = nodesMap.get(qdbNode).size();
            while (startIndex < numberOfKeys) {
                int endIndex = Math.min(numberOfKeys, getSplitSize(nodesMap.get(qdbNode).size(), QuasardbJobConf.getHadoopClusterSize(conf, 3)) + startIndex);
                final List<String> split = nodesMap.get(qdbNode).subList(startIndex, endIndex);
                splits.add(new QuasardbInputSplit(split, new QuasardbNode[] { qdbNode }));
                startIndex = endIndex;
            }
        }
        return splits;
    }
    
    /**
     * Calculates the split size.
     * <br>
     * Uses an heuristic to generate ~10 splits per hadoop node. 
     * <br>
     * Falls back to some lower number if the inputs are smaller, and lower still when there are less inputs than hadoop nodes.
     * <br>
     *  
     * @param numberOfKeys total input size
     * @param hadoopClusterSize number of nodes in the hadoop m/r cluster
     * @return the size for each split
     * @see <a href="http://wiki.apache.org/hadoop/HowManyMapsAndReduces">Partitioning your job into maps and reduces</a>
     * @since 1.3.0
     */
    public int getSplitSize(int numberOfKeys, int hadoopClusterSize) {
        int splitSize = numberOfKeys / (hadoopClusterSize * 10);
        if (splitSize < QuasardbJobConf.getMinSplitSize()) {
            // too few? then use a smaller divider
            splitSize = numberOfKeys / hadoopClusterSize;
            if (splitSize < QuasardbJobConf.getMinSplitSize()) {
                // still too few? just split into splits of minimum split size
                splitSize = QuasardbJobConf.getMinSplitSize();
            }
        }
        return splitSize;
    }
}
