package com.b14.qdb.hadoop.mapreduce.examples.financial;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import com.b14.qdb.hadoop.mapreduce.keysgenerators.ProvidedKeysGenerator;
import com.b14.qdb.hadoop.mapreduce.tools.FixHadoopOnWindows;

/**
 * Hadoop MapReduce example showing high and low for a stock symbol
 *
 */
public class HighLowStockIT extends Configured implements Tool {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 2836;
    private static final QuasardbConfig config = new QuasardbConfig();
    private Quasardb qdbInstance = null;
    private String keys = new String();
    
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(HighLowStockIT.class);
        job.setJobName("High Low per Stock");

        job.setInputFormatClass(QuasardbInputFormat.class);
        job.setOutputFormatClass(QuasardbOutputFormat.class);
        
        job.setMapperClass(HighLowStockMapper.class);
        job.setReducerClass(HighLowStockReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        HighLowStockIT driver = new HighLowStockIT();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
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
            String line = "";
            InputStream is = getClass().getClassLoader().getResourceAsStream("NASDAQ_daily_prices_Y.csv");
            InputStreamReader fis = new InputStreamReader(is);
            BufferedReader reader = new BufferedReader(fis);
            long i = 0;
            while ((line = reader.readLine()) != null) {
                i++;
                if (i == 1) {
                    continue;
                }
                qdbInstance.update("key_" + i, new Text(line));
                if (!keys.isEmpty()) {
                    keys += ",";
                }
                keys += "key_" + i;
            }
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
                // Check results
                Text result = qdbInstance.get("YDNT");
                assertTrue("High:39.66 Low:7.54".equals(result.toString().trim()));
                result = qdbInstance.get("YHOO");
                assertTrue("High:118.75 Low:0.66".equals(result.toString().trim()));
                
                // Clean
                qdbInstance.purgeAll();
                qdbInstance.close();
                
            }
        } catch (QuasardbException e) {
            fail("Shouldn't have an Exception.");
        }
    }
    
    @Test
    public void testHighLowStockDriver() throws Throwable {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.setInt("dfs.replication", 1);
        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("hadoop.tmp.dir", FixHadoopOnWindows.isWindows() ? "D:/tmp" : "/tmp");
        conf = QuasardbJobConf.addLocation(conf, new QuasardbNode(HOST, PORT));
        ProvidedKeysGenerator providedKeysGenerator = new ProvidedKeysGenerator();
        providedKeysGenerator.init(keys);
        conf = QuasardbJobConf.setKeysGenerator(conf, providedKeysGenerator);
        conf = QuasardbJobConf.setHadoopClusterSize(conf, 4);
        
        Job job = new Job(conf, "High Low per Day");
        job.setJarByClass(HighLowStockIT.class);
        job.setJobName("High Low per Day");
        
        job.setInputFormatClass(QuasardbInputFormat.class);
        job.setOutputFormatClass(QuasardbOutputFormat.class);

        job.setMapperClass(HighLowStockMapper.class);
        job.setReducerClass(HighLowStockReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}