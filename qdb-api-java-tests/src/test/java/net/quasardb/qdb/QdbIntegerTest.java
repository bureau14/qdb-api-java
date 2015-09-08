package net.quasardb.qdb;

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
 * @version 2.0.0
 * @since 2.0.0
 */
public class QdbIntegerTest {
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
     * Test of method {@link QdbInteger#QdbInteger(net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session, String, int)}.
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
     * Test of method {@link QdbInteger#QdbInteger(net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session, String, int)}.
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
     * Test of method {@link QdbInteger#QdbInteger(net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session, String, int, long)}.
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
            assertEquals(qdbInt.getAlias(), "testInteger");
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
     * Test of method {@link QdbInteger#add(int)}.
     *
     * @throws QdbException
     */
    @Test
    public void testAdd() throws QdbException {
        QdbInteger qdbInt = cluster.getInteger("testInteger");
        qdbInt.put(1);
        try {
            assertEquals(3, qdbInt.add(2));
            assertEquals(3,qdbInt.get());
            assertEquals(-1,qdbInt.add(-4));
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
        qdbInt.remove();
        try {
            qdbInt.get();
            fail("Entry should have been removed.");
        } catch (Exception e) {
            assertTrue(e instanceof QdbException);
        }
    }

    private void cleanUp(QdbInteger qdbInt) {
        try {
            qdbInt.expiresFromNow(0);
        } catch (QdbException e) {
        }
    }
}
