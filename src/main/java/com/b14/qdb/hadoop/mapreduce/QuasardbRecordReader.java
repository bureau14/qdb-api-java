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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;

/**
 * Wrapper around a {@link Quasardb} for reading values from quasardb.
 * 
 * @param <V> value
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see RecordReader
 * @version master
 * @since 1.3.0
 */
public class QuasardbRecordReader<V> extends RecordReader<Text, V> {
    private final Quasardb client;
    private ConcurrentLinkedQueue<String> keys;
    private long initialSize;

    public QuasardbRecordReader() {
        this.client = new Quasardb();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     * @since 1.3.0
     */
    @Override
    public void close() throws IOException {
        try {
            if (client != null) {
                client.close();
            }
        } catch (QuasardbException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     * @since 1.3.0
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        Text currentKey = new Text();
        currentKey.set(keys.peek());
        return currentKey;
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     * @since 1.3.0
     */
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        V result = null;
        try {
            if ((client != null) && (keys != null)) {
                result = client.get(keys.poll());
            }
        } catch (QuasardbException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     * @since 1.3.0
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (keys == null) {
            return 0;
        }
        int size = keys.size();
        if (size == 0) {
            return 0;
        } else {
            return (float) size / initialSize;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(InputSplit, TaskAttemptContext)
     * @since 1.3.0
     */
    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        if (split == null) {
            throw new IOException("Provided InputSplit should not be null.");
        }
        
        try {
            final QuasardbInputSplit inputSplit = (QuasardbInputSplit) split;
            final QuasardbConfig qdbConf = new QuasardbConfig();
            this.keys = new ConcurrentLinkedQueue<String>(inputSplit.getInputs());
            this.initialSize = split.getLength();
            for (QuasardbNode node : inputSplit.getQdbLocations()) {
                qdbConf.addNode(node);
            }
            this.client.setConfig(qdbConf);

            // Try to connect to node
            this.client.connect();
        } catch (QuasardbException e) {
            throw new IOException("Cannot connect to provided node => " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new IOException("Node port is invalid => " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Node hostname is invalid => " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     * @since 1.3.0
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return keys.peek() != null;
    }
}
