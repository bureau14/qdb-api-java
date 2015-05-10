package com.b14.qdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A integration tests case for {@link QdbInteger} class.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version master
 * @since 2.0.0
 */
public class QdbIntegerIT {
    private static final String URI = "qdb://127.0.0.1:2836";
    private QdbCluster cluster = null;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Qdb.DAEMON.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Qdb.DAEMON.stop();
    }

    @Before
    public void setUp() {
        try {
            cluster = new QdbCluster(URI);
            cluster.purgeAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            cluster.purgeAll();
        } catch (Exception e) {
        }
    }
    
    /**
     * Test of method {@link QdbInteger#QdbInteger(com.b14.qdb.jni.SWIGTYPE_p_qdb_session, String, int)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testQdbIntegerDefault() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put();
        try {
            assertTrue("Defaut value should be 0.", qdbInt.get() == 0);
            assertTrue("Defaut expiry time should be 0 (aka eternal).", qdbInt.getExpiryTime() == 0);
        } finally {
            // Clean up
            qdbInt.expiresFromNow(0);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#QdbInteger(com.b14.qdb.jni.SWIGTYPE_p_qdb_session, String, int)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testQdbIntegerWithInitialValue() throws QdbException {
        int initialValue = 10;
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(initialValue);
        try {
            assertTrue("Initial value should be equals to " + initialValue, qdbInt.get() == initialValue);
            assertTrue("Defaut expiry time should be 0 (aka eternal).", qdbInt.getExpiryTime() == 0);
        } finally {
            // Clean up
            qdbInt.expiresFromNow(0);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#QdbInteger(com.b14.qdb.jni.SWIGTYPE_p_qdb_session, String, int, long)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testQdbIntegerWithInitialValueAndExpiryTime() throws QdbException {
        int initialValue = 10;
        long expiryTimeInSeconds = 2L;
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(initialValue, expiryTimeInSeconds);
        assertTrue("Initial value should be equals to " + initialValue, qdbInt.get() == initialValue);
        assertTrue("Expiry time shouldn't be equals to 0 (aka eternal).", qdbInt.getExpiryTime() != 0);
        
        try {
            Thread.sleep((expiryTimeInSeconds + 1) * 1000);
        } catch (Exception e) {
        }
        
        try {
            qdbInt.get();
            fail("Entry should have been expired");
        } catch (Exception e) {
            assertTrue(e instanceof QdbException);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#alias()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testAlias() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put();
        try {
            assertEquals(0, qdbInt.get());
            assertEquals(qdbInt.alias(), "testInteger");
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#get()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testGetSet() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put();
        try {
            assertEquals(0, qdbInt.get());
            qdbInt.set(2);
            assertEquals(2, qdbInt.get());
            qdbInt.set(-3);
            assertEquals(-3, qdbInt.get());
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#getAndSet(int)}.
     * 
     * @throws QdbException
     */
    public void testGetAndSet() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(1,qdbInt.getAndSet(0));
            assertEquals(0,qdbInt.getAndSet(-10));
            assertEquals(-10,qdbInt.getAndSet(1));
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#getAndIncrement()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testGetAndIncrement() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(1,qdbInt.getAndIncrement());
            assertEquals(2,qdbInt.get());
            qdbInt.set(-2);
            assertEquals(-2,qdbInt.getAndIncrement());
            assertEquals(-1,qdbInt.getAndIncrement());
            assertEquals(0,qdbInt.getAndIncrement());
            assertEquals(1,qdbInt.get());
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#getAndAdd(int)}.
     * 
     * @throws QdbException
     */
    public void testGetAndAdd() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(1,qdbInt.getAndAdd(2));
            assertEquals(3,qdbInt.get());
            assertEquals(3,qdbInt.getAndAdd(-4));
            assertEquals(-1,qdbInt.get());
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#incrementAndGet()}.
     * 
     * @throws QdbException
     */
    public void testIncrementAndGet() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(2,qdbInt.incrementAndGet());
            assertEquals(2,qdbInt.get());
            qdbInt.set(-2);
            assertEquals(-1,qdbInt.incrementAndGet());
            assertEquals(0,qdbInt.incrementAndGet());
            assertEquals(1,qdbInt.incrementAndGet());
            assertEquals(1,qdbInt.get());
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#addAndGet(int)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testAddAndGet() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(3,qdbInt.addAndGet(2));
            assertEquals(3,qdbInt.get());
            assertEquals(-1,qdbInt.addAndGet(-4));
            assertEquals(-1,qdbInt.get());
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#expiresAt(java.util.Date)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testExpiresAt() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            long time = System.currentTimeMillis() + (1000 * 60 * 60);
            Date expiryDate = new Date(time);
            qdbInt.expiresAt(expiryDate);
            
            String sTime = "" + time;
            assertTrue("Expiry time for entry[test_expiry_1] should be equals to computed date => " + sTime.substring(0, sTime.length() - 3), (qdbInt.getExpiryTime() + "").equals(sTime.substring(0, sTime.length() - 3)));
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#expiresFromNow(long)}.
     * 
     * @throws QdbException
     */
    @Test
    public void testExpiresFromNow() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        assertTrue(qdbInt.get() == 1);
        qdbInt.expiresFromNow(0);
        try {
            Thread.sleep(500L);
        } catch (Exception e) {
        }
        try {
            qdbInt.get();
            fail("Entry should have been expired");
        } catch (Exception e) {
            assertTrue(e instanceof QdbException);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#remove()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testRemove() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        assertTrue(qdbInt.get() == 1);
        assertTrue(qdbInt.remove());
        try {
            qdbInt.get();
            fail("Entry should have been removed.");
        } catch (Exception e) {
            assertTrue(e instanceof QdbException);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#toString()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testToString() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            for (int i = -12; i < 6; ++i) {
                qdbInt.set(i);
                assertEquals(qdbInt.toString(), Integer.toString(i));
            }
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    /**
     * Test of method {@link QdbInteger#intValue()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testIntValue() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            for (int i = -12; i < 6; ++i) {
                qdbInt.set(i);
                assertEquals(i, qdbInt.intValue());
            }
        } finally {
        	cleanUp(qdbInt);
        }
    }


    /**
     * Test of method {@link QdbInteger#longValue()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testLongValue() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            for (int i = -12; i < 6; ++i) {
                qdbInt.set(i);
                assertEquals((long)i, qdbInt.longValue());
            }
        } finally {
        	cleanUp(qdbInt);
        }
    }

    /**
     * Test of method {@link QdbInteger#floatValue()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testFloatValue() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            for (int i = -12; i < 6; ++i) {
                qdbInt.set(i);
                assertEquals((float)i, qdbInt.floatValue(), 0);
            }
        } finally {
        	cleanUp(qdbInt);
        }
    }

    /**
     * Test of method {@link QdbInteger#doubleValue()}.
     * 
     * @throws QdbException
     */
    @Test
    public void testDoubleValue() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            for (int i = -12; i < 6; ++i) {
                qdbInt.set(i);
                assertEquals((double)i, qdbInt.doubleValue(), 0);
            }
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    @Test
    public void testThreads() throws QdbException {
        final QdbInteger qdbInt = cluster.getInteger("testThreads");
        qdbInt.put(1);
        
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    while (qdbInt.get() <= 2) {
                        qdbInt.getAndIncrement();
                        Thread.yield();
                    }
                } catch (QdbException e) {
                    e.printStackTrace();
                }
            }
        });
        
        try {
            t.start();
            qdbInt.getAndIncrement();
            t.join(50 * 200);
            assertTrue(!t.isAlive());
            assertEquals(qdbInt.get(), 3);
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        } finally {
        	cleanUp(qdbInt);
        }
    }
    
    private void cleanUp(QdbInteger qdbInt) {
        try {
			qdbInt.expiresFromNow(0);
		} catch (QdbException e) {
		}
    }
}
