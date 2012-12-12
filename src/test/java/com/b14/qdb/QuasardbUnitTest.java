package com.b14.qdb;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdbJNI;
import com.b14.qdb.jni.qdb_error_t;

@RunWith(PowerMockRunner.class)
@PrepareForTest({qdb.class, qdbJNI.class})
@SuppressStaticInitializationFor("com.b14.quasardb.QuasarDB")
public class QuasardbUnitTest {
    private static SWIGTYPE_p_qdb_session session = null;
    private Quasardb qdbDB = null;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @SuppressWarnings("rawtypes")
    @Before
    public void setUp() {
        mockStatic(qdbJNI.class);
        when(qdbJNI.error_ok_get()).thenReturn(1);
        
        mockStatic(qdb.class);
        when(qdb.open()).thenReturn(session);
        when(qdb.connect(session, "mock", 2836)).then(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                return qdb_error_t.error_ok;
            }
        });
        
        qdbDB = new Quasardb();
    }
    
    @After
    public void tearDown() {
        
    }

    @Test
    public void testConnect() {
       // Wrong params
        try {
            qdbDB.connect();
            fail("No arg should fail");
        } catch (QuasardbException e) {
        }
        
        Map<String,String> configMock = new HashMap<String,String>();
        qdbDB.setConfig(configMock);
        try {
            qdbDB.connect();
            fail("No args should fail");
        } catch (QuasardbException e) {
        }
        
        configMock.put("host", "");
        configMock.put("port", "2836");
        qdbDB.setConfig(configMock);
        try {
            qdbDB.connect();
            fail("Empty args should fail");
        } catch (QuasardbException e) {
        }
        
        configMock.remove("host");
        configMock.remove("port");
        configMock.put("host", "mock");
        configMock.put("port", "mock");
        qdbDB.setConfig(configMock);
        try {
            qdbDB.connect();
            fail("Wrong port args should fail");
        } catch (QuasardbException e) {
        }
        
        // Nominal case
        configMock.remove("host");
        configMock.remove("port");
        configMock.put("host", "mock");
        configMock.put("port", "2836");        
        qdbDB.setConfig(configMock);
        try {
            qdbDB.connect();
        } catch (QuasardbException e) {
            fail("Nominal case don't fail.");
        }
        
        // No host
        configMock.remove("host");
        configMock.remove("port");
        configMock.put("host", "127.0.0.1");
        configMock.put("port", "666");        
        qdbDB.setConfig(configMock);
        try {
            qdbDB.connect();
            fail("No server should fail.");
        } catch (QuasardbException e) {
        }
    }

    @Test
    public void testGet() {
        Map<String,String> configMock = new HashMap<String,String>();
        qdbDB.setConfig(configMock);
        configMock.remove("host");
        configMock.remove("port");
        configMock.put("host", "mock");
        configMock.put("port", "2836");
        
        
    }

    @Test
    public void testPut() {
        fail("Not yet implemented");
    }

    @Test
    public void testUpdate() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetAndReplace() {
        fail("Not yet implemented");
    }

    @Test
    public void testCompareAndSwap() {
        fail("Not yet implemented");
    }

    @Test
    public void testRemove() {
        fail("Not yet implemented");
    }

    @Test
    public void testRemoveAll() {
        fail("Not yet implemented");
    }

    @Test
    public void testClose() {
        fail("Not yet implemented");
    }
    
}
