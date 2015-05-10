package com.b14.qdb;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.net.URISyntaxException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

/**
 * A unit test case for {@link QdbCluster} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.6
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    LibraryHelper.class,
    qdbJNI.class,
    qdb_error_t.class
})
public class QdbClusterTest {
    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws QdbException {
        PowerMockito.mockStatic(LibraryHelper.class);
        PowerMockito.mockStatic(qdbJNI.class);
        PowerMockito.mockStatic(qdb_error_t.class);
        PowerMockito.mockStatic(qdb.class);

        BDDMockito.given(qdb_error_t.swigToEnum(any(int.class))).willReturn(qdb_error_t.error_ok);
        BDDMockito.given(qdbJNI.open()).willReturn(123L);
        BDDMockito.given(qdbJNI.connect(any(long.class), any(String.class))).willReturn(1);
        BDDMockito.given(qdbJNI.close(any(long.class))).willReturn(1);
    }

    @After
    public void tearDown() {
    }
    
    /**
     * Test of method {@link QdbCluster#QdbCluster(String)}.
     * @throws QuasardbException
     */
    @Test
    public void testWrongURI() {
        try {
            new QdbCluster("wrong_uri");
        } catch (Exception e) {
            assertTrue("Exception should be a URISyntaxException => " + e, e instanceof URISyntaxException);
        }
    }
}
