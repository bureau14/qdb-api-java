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
package com.b14.qdb.hadoop.mapreduce.examples.wordcounter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mapreduce.QuasardbInputFormat;
import com.b14.qdb.hadoop.mapreduce.QuasardbJobConf;
import com.b14.qdb.hadoop.mapreduce.QuasardbOutputFormat;
import com.b14.qdb.hadoop.mapreduce.examples.wordcounter.WordCountJob.TokenCounterMapper;
import com.b14.qdb.hadoop.mapreduce.examples.wordcounter.WordCountJob.TokenCounterReducer;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.ProvidedKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.tools.FixHadoopOnWindows;

/**
 * A integration test case for <code><a href="{@docRoot}/com/b14/qdb/hadoop/mapreduce/package-summary.html">com.b14.qdb.hadoop.mapreduce</a></code> classes.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class WordCountIT {
    private static final Charset CHARSET = Charset.forName("UTF8");
    private static final CharsetDecoder DECODER = CHARSET.newDecoder();
    
    private static final String AUTHOR = "Mark Twain";
    private static final String BOOK = "Adventures of Huckleberry Finn";
    private static final Pattern START_PATTERN = Pattern.compile("\\*\\*\\* START.*\\*\\*\\*");
    private static final Pattern CHAPTER_PATTERN = Pattern.compile("CHAPTER\\s+([IVXLCDMTHEAST\\p{Blank}]+)[\\.|\r|\n]");
    private static final Pattern END_PATTERN = Pattern.compile("\\*\\*\\* END");
    
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 2836;
    private static final QuasardbConfig config = new QuasardbConfig();
    private Quasardb qdbInstance = null;
    private String keys = new String();
    
    public WordCountIT() {
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        QuasardbNode quasardbNode = new QuasardbNode(HOST, PORT);
        config.addNode(quasardbNode);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        //  => Connect to given qdbinstance
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        
        //  => Populate quasardb instance with data
        try {
            URL resourceUrl = getClass().getClassLoader().getResource("huck_fin.txt");
            FileInputStream fis = new FileInputStream(new File(resourceUrl.toURI()));
            FileChannel channel = fis.getChannel();
            
            int fileSize = (int) channel.size();
            MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            CharBuffer charBuffer = DECODER.decode(byteBuffer);
    
            Matcher chapterMatcher = CHAPTER_PATTERN.matcher(charBuffer);
            Matcher startMatcher = START_PATTERN.matcher(charBuffer);
            Matcher endMatcher = END_PATTERN.matcher(charBuffer);
    
            boolean match = startMatcher.find() && chapterMatcher.find();
            int from = startMatcher.end();
            endMatcher.find();
            int end = endMatcher.start();
    
            while (match) {
                String key = chapterMatcher.group(1).replaceAll("\\s", "");
                match = chapterMatcher.find();
                int to;
    
                if (!match) {
                    to = end;
                } else {
                    to = chapterMatcher.start();
                }

                Chapter c = new Chapter(AUTHOR, BOOK, key, charBuffer.subSequence(from, to).toString());
                qdbInstance.put(key, c);
                if (!keys.isEmpty()) {
                    keys += ",";
                }
                keys += key;
                from = to;
            }
            System.out.println("*** End of loading data => keys are " + keys);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't raise an Exception during data loading..." + e.getMessage());
        }
        
        // => Fix Hadoop on Windows
        try {
            FixHadoopOnWindows.runFix();
        } catch (Exception e) {
        }
    }

    @After
    public void tearDown() {
        try {
            if (qdbInstance != null) {
                // Do some checkups
                WordCountResult result = qdbInstance.get("and");
                assertTrue("Word 'and' should be the word 'and'", result.getWord().equals("and"));
                assertTrue("Word 'and' should be counted 6218th.", result.getCount() == 6218);
                
                result = qdbInstance.get("ain't");
                assertTrue("Word 'ain't' should be the word 'ain't'", result.getWord().equals("ain't"));
                assertTrue("Word 'ain't' should be counted 293th.", result.getCount() == 293);

                result = qdbInstance.get("about");
                assertTrue("Word 'about' should be the word 'about'", result.getWord().equals("about"));
                assertTrue("Word 'about' should be counted 419th.", result.getCount() == 419);
                
                // Clean
                qdbInstance.purgeAll();
                qdbInstance.close();
            }
        } catch (QuasardbException e) {} 
    }
    
    @Test
    public void testWordCount() throws Throwable {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.setInt("dfs.replication", 1);
        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("hadoop.tmp.dir", FixHadoopOnWindows.isWindows() ? "D:/tmp" : "/tmp");
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode("127.0.0.1", 2836));
        ProvidedKeysGenerator providedKeysGenerator = new ProvidedKeysGenerator();
        providedKeysGenerator.init(keys);
        conf = QuasardbJobConf.setKeysGenerator(conf, providedKeysGenerator);
        conf = QuasardbJobConf.setHadoopClusterSize(conf, 4);
        
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(WordCountIT.class);
        job.setInputFormatClass(QuasardbInputFormat.class);
        job.setMapperClass(TokenCounterMapper.class);
        
        job.setReducerClass(TokenCounterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(QuasardbOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordCountResult.class);
        
        job.setNumReduceTasks(4);

        job.submit();
        job.waitForCompletion(true);
    }
}
