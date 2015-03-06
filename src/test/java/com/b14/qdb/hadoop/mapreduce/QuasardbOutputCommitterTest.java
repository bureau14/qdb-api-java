package com.b14.qdb.hadoop.mapreduce;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A unit test case for {@link QuasardbOutputCommitter} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbOutputCommitterTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link QuasardbOutputCommitter#abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testAbort() {
        // Nothing to test
    }

    /**
     * Test method for {@link QuasardbOutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testCommitTask() {
        // Nothing to test
    }

    /**
     * Test method for {@link QuasardbOutputCommitter#needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testNeedsTaskCommit() {
        QuasardbOutputCommitter qdbOutputCommitter = new QuasardbOutputCommitter();
        try {
            assertFalse("Should return false.", qdbOutputCommitter.needsTaskCommit(null));
        } catch (IOException e) {
            fail("No Exception allowed.");
        }
    }

    /**
     * Test method for {@link QuasardbOutputCommitter#setupJob(org.apache.hadoop.mapreduce.JobContext)}.
     */
    @Test
    public void testSetupJob() {
        // Nothing to test
    }

    /**
     * Test method for {@link QuasardbOutputCommitter#setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext)}.
     */
    @Test
    public void testSetupTask() {
        // Nothing to test
    }

}
