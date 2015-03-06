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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;

/**
 * Writes reducer results to quasardb.
 * 
 * @param <V> value
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see RecordWriter
 * @version master
 * @since 1.3.0
 */
public class QuasardbRecordWriter<V> extends RecordWriter<Text, V> {
    private final Quasardb client;
    
    /**
     * Build a QuasardbRecordWriter with provided context
     * 
     * @param taskAttemptContext
     * @throws QuasardbException if client couldn't connect to provided instance
     * @throws IOException if provided configuration is wrong (bad hostname or port) or null
     */
    public QuasardbRecordWriter(TaskAttemptContext taskAttemptContext) throws QuasardbException, IOException {
        final Configuration conf = taskAttemptContext.getConfiguration();
        final QuasardbConfig qdbConf = new QuasardbConfig();
        for (QuasardbNode node : QuasardbJobConf.getNodesLocation(conf)) {
            qdbConf.addNode(node);
        }
        this.client = new Quasardb(qdbConf);
        this.client.connect();
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     * @since 1.3.0
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     * @since 1.3.0
     */
    @Override
    public void write(Text key, V value) throws IOException, InterruptedException {
        try {
            if ((key == null) || (value == null)) {
                throw new IOException("Key or Value is null : " + key + " => " + value);
            }
            if (client != null) {
                client.update(key.toString(), value);
            }
        } catch (QuasardbException e) {
            throw new IOException(e);
        }
    }

}
