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
package com.b14.qdb.hadoop.mapreduce.examples.standarddeviation;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.Qdb;
import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mapreduce.QuasardbInputFormat;
import com.b14.qdb.hadoop.mapreduce.QuasardbJobConf;
import com.b14.qdb.hadoop.mapreduce.QuasardbOutputFormat;
import com.b14.qdb.hadoop.mapreduce.keysgenerators.ProvidedKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.tools.FixHadoopOnWindows;

public class StandardDeviationIT {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 2836;
    private static final QuasardbConfig config = new QuasardbConfig();
    private Quasardb qdbInstance = null;
    private String keys = new String();
    
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
        //  => Connect to given qdbinstance
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        
        //  => Populate quasardb instance with data
        try {
            String line = "";
            InputStream is = getClass().getClassLoader().getResourceAsStream("01_heights_weights_genders.csv");
            InputStreamReader fis = new InputStreamReader(is);
            BufferedReader reader = new BufferedReader(fis);
            long i = 0;
            while ((line = reader.readLine()) != null) {
                i++;
                if (i == 1) {
                    continue;
                }
                String[] tokens = line.split(",");
                String gender = tokens[0];
                double height = Double.parseDouble(tokens[1]);
                double weight = Double.parseDouble(tokens[2]);
                qdbInstance.put("key_" + i, new People(gender, height, weight));
                if (!keys.isEmpty()) {
                    keys += ",";
                }
                keys += "key_" + i;
            }
            System.out.println("*** End of loading data => keys are " + keys);
        } catch (Exception e) {}
        
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
                double[] values = qdbInstance.get("all_people");
                double count = values[2];
                double sum = values[0];
                double sqrt = values[1];
                
                assertTrue("Count should be 10000", count == 10000L);
                
                System.out.println("Count = " + count);
                System.out.println("Sum = " + sum);
                System.out.println("Sum of Sqrt = " + sqrt);

                double result = Math.sqrt((sqrt - (Math.pow(sum, 2) / count)) / (count - 1));
                assertTrue("Standard deviation on sample people should be 3.84.", result == 3.847528120774169);
                System.out.println(" ===> std dev = " + result);
                
                // Clean
                qdbInstance.purgeAll();
                qdbInstance.close();
            }
        } catch (QuasardbException e) {}
    }
    
    @Test
    public void testStandardDeviation() throws Throwable {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.setInt("dfs.replication", 1);
        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("hadoop.tmp.dir", FixHadoopOnWindows.isWindows() ? System.getProperty("java.io.tmpdir") : "/tmp");
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode("127.0.0.1", 2836));
        ProvidedKeysGenerator providedKeysGenerator = new ProvidedKeysGenerator();
        providedKeysGenerator.init(keys);
        conf = QuasardbJobConf.setKeysGenerator(conf, providedKeysGenerator);
        conf = QuasardbJobConf.setHadoopClusterSize(conf, 4);
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        
        Job job = new Job(conf, "StandardDeviation");
        job.setJarByClass(StandardDeviationIT.class);
        job.setInputFormatClass(QuasardbInputFormat.class);
        job.setOutputFormatClass(QuasardbOutputFormat.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(double[].class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(double[].class);
        
        job.setNumReduceTasks(4);

        job.submit();
        job.waitForCompletion(true);
    }
    
    public static class Map extends Mapper<Text, People, Text, double[]> {
        public void map(Text key, People value, Context context) throws IOException, InterruptedException {
            double[] val = new double[3];
            val[0] = value.getHeight();
            val[1] = Math.pow(val[0], 2);
            val[2] = 1;
            context.write(new Text("all_people"), val);
        }
    }
    
    public static class Reduce extends Reducer<Text, double[], Text, double[]> {
        public void reduce(Text key, Iterable<double[]> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double sumsqrt = 0;
            double counter = 0;

            for (double[] val : values) {
                counter += val[2];
                sum += val[0];
                sumsqrt += val[1];
            }

            double[] newval = new double[3];
            newval[0] = sum;
            newval[1] = sumsqrt;
            newval[2] = counter;
            context.write(key, newval);
        }
    }
}
