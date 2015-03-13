package com.b14.qdb.hadoop.mapreduce.examples.wordcounter;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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

/**
 * Hadoop MapReduce example extracting all letters from hamlet text and running on quasardb
 * 
 */
public class StartsWithCountIT extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        // Populate quasardb
        QuasardbConfig config = new QuasardbConfig();
        config.addNode(new QuasardbNode("127.0.0.1", 2836));
        Quasardb qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        InputStream is = getClass().getClassLoader().getResourceAsStream("hamlet.txt");
        InputStreamReader fis = new InputStreamReader(is);
        BufferedReader reader = new BufferedReader(fis);
        String content = "";
        try {
            StringBuilder sb = new StringBuilder();
            String line = reader.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = reader.readLine();
            }
            content = sb.toString();
        } finally {
            reader.close();
        }
        qdbInstance.update("hamlet", new Text(content));
        
        // configure connection to quasardb
        Configuration conf = new Configuration();
        conf.set(QuasardbJobConf.QDB_NODES, "127.0.0.1:2836");
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode("127.0.0.1", 2836));
        conf.set("fs.default.name", "file:///");
        conf.setInt("dfs.replication", 1);
        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("hadoop.tmp.dir", FixHadoopOnWindows.isWindows() ? System.getProperty("java.io.tmpdir") : "/tmp");
        ProvidedKeysGenerator providedKeysGenerator = new ProvidedKeysGenerator();
        providedKeysGenerator.init("hamlet,");
        conf = QuasardbJobConf.setKeysGenerator(conf, providedKeysGenerator);
        conf = QuasardbJobConf.setHadoopClusterSize(conf, 4);
        
        Job job = Job.getInstance(conf, "StartsWithCount") ;
        job.setJarByClass(getClass());
        
        // configure output and input source
        job.setInputFormatClass(QuasardbInputFormat.class);
        
        // configure mapper and reducer
        job.setMapperClass(StartsWithCountMapper.class);
        job.setCombinerClass(StartsWithCountReducer.class);
        job.setReducerClass(StartsWithCountReducer.class);  
    
        // configure output
        job.setOutputFormatClass(QuasardbOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {        
        // Launch Mapreduce Job
        int exitCode = ToolRunner.run(new StartsWithCountIT(), args);
        System.exit(exitCode);
    }
    
    @Test
    public void testStartsWithCount() {
        Qdb.DAEMON.start();
        try {
            FixHadoopOnWindows.runFix();
            int exitCode = ToolRunner.run(new StartsWithCountIT(), null);
            assertTrue(exitCode == 0);
            
            QuasardbConfig config = new QuasardbConfig();
            config.addNode(new QuasardbNode("127.0.0.1", 2836));
            Quasardb qdbInstance = new Quasardb(config);
            try {
                qdbInstance.connect();
            } catch (QuasardbException e) {
                e.printStackTrace();
            }
            IntWritable count = null;
            count = qdbInstance.get("a");
            assertTrue(count.get() == 2451);
            count = qdbInstance.get("m");
            assertTrue(count.get() == 1835);
            
            qdbInstance.purgeAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Qdb.DAEMON.stop();
        }
    }
}
