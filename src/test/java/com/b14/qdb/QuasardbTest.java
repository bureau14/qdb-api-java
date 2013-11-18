/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
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
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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

package com.b14.qdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.data.Pojo;
import com.b14.qdb.entities.NodeConfig;
import com.b14.qdb.entities.NodeStatus;
import com.b14.qdb.entities.NodeTopology;
import com.b14.qdb.entities.QuasardbEntry;

public class QuasardbTest {
    public static final String HOST = "127.0.0.1";
    public static final String PORT = "2836";
    private static final Map<String,String> config = new HashMap<String,String>();
    
    private Quasardb qdbInstance = null;

    public QuasardbTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {        
        config.put("name", "test");
        config.put("host", HOST);
        config.put("port", PORT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
        qdbInstance = new Quasardb(config);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
    }
    
    @After
    public void tearDown() {
    }
    
    /**
     * Test of close method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testClose() throws Exception {
        // Test : close qdb session
        qdbInstance.close();
        
        try {
            qdbInstance.put("test_close", "test_close");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // re-initialize qdb session
        setUpClass();
    }

    /**
     * Test of put method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testPut() throws QuasardbException {
        // Test 1.1 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.put("test_put_1", test);
        String result = qdbInstance.get("test_put_1");
        assertTrue(test.equals(result));
        
        // Test 1.2 : nominal case - pojo
        Pojo pojo = new Pojo();
        qdbInstance.put("test_put_2", pojo);
        Pojo pojoresult = qdbInstance.get("test_put_2");
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        
        // Test 2.1 : wrong parameter
        try {
            qdbInstance.put("test_put_3", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
       
        // Test 2.2 : wrong parameter
        try {
            qdbInstance.put(null, pojo);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.3 : wrong parameter
        try {
            qdbInstance.put(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.4 : wrong parameter -> alias too long
        String veryLongAlias = "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj";
        try {
            qdbInstance.put(veryLongAlias, "test long alias");
            assertTrue(qdbInstance.get(veryLongAlias).equals("test long alias"));
        } catch (Exception e) {
            fail("Very long alias are allowed now.");
        }
          
        // Test 3 : put with a key already mapped
        try {
            qdbInstance.put("test_put_2", "test_put_2 key is already mapped");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup Qdb
        qdbInstance.remove("test_put_1");
        qdbInstance.remove("test_put_2");
        qdbInstance.remove(veryLongAlias);
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testGet() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.get(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.get("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.get("test_nominal");
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        
        // Test 3 : wrong alias
        try {
            qdbInstance.get("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testGetAndReplace() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.getAndReplace(null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getAndReplace("test1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getAndReplace("", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getAndReplace("test_nominal", pojo2);
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo2.getText().equals(pojoGet.getText()));
        
        // Test 3 : wrong alias
        try {
            qdbInstance.getAndReplace("alias_doesnt_exist", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup
        qdbInstance.remove("test_nominal");
    }
    
    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testGetRemove() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.getRemove(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.getRemove("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : nominal case
        Pojo pojo = new Pojo();
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.getRemove("test_nominal");
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        try {
            qdbInstance.get("test_nominal");
            fail("Entry must not exist anymore");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 3 : wrong alias
        try {
            qdbInstance.getRemove("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }  
    }
    
    /**
     * Test of get method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testCompareAndSwap() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.compareAndSwap(null, "test", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("test1", null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("test", "test", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.4 : test wrong parameter
        try {
            qdbInstance.compareAndSwap("", "test", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.1 : nominal case -> swap case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        Pojo pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo);
        assertTrue(pojo.getText().equals(pojoresult.getText()));
        Pojo pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo2.getText().equals(pojoGet.getText()));
        assertFalse(pojo.getText().equals(pojoGet.getText()));
        qdbInstance.remove("test_nominal");
        pojoresult = null;
        pojoGet = null;
        
        // Test 2.2 : nominal case -> no swap case
        qdbInstance.put("test_nominal", pojo);
        Pojo pojo3 = new Pojo();
        pojo3.setText("test3");
        pojoresult = qdbInstance.compareAndSwap("test_nominal", pojo2, pojo3);
        assertFalse(pojo3.getText().equals(pojoresult.getText()));
        pojoGet = qdbInstance.get("test_nominal");
        assertTrue(pojo.getText().equals(pojoGet.getText()));
        
        // Test 3 : wrong alias
        try {
            qdbInstance.compareAndSwap("alias_doesnt_exist", "test","test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup
        qdbInstance.remove("test_nominal");
    }
    
    /**
     * Test of delete method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testRemove() throws QuasardbException {      
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.remove(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.remove("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.remove("alias_doesnt_exist");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.put("test_del_1", test);
        qdbInstance.remove("test_del_1");
        try {
            qdbInstance.get("test_del_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of delete method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testRemoveAll() throws QuasardbException {
        // Test : nominal case - add 4 objects and remove them all
        String test = "Voici un super test";
        qdbInstance.put("test_del_1", test);
        qdbInstance.put("test_del_2", test);
        qdbInstance.put("test_del_3", test);
        qdbInstance.put("test_del_4", test);
        qdbInstance.removeAll();
        try {
            qdbInstance.get("test_del_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_2");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_3");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        try {
            qdbInstance.get("test_del_4");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    }
    
    /**
     * Test of removeIf method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testRemoveIf() throws QuasardbException {
        // Test 1.1 : test wrong parameter
        try {
            qdbInstance.removeIf(null, "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.2 : test wrong parameter
        try {
            qdbInstance.removeIf("test1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 1.3 : test wrong parameter
        try {
            qdbInstance.removeIf(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.1 : nominal case -> remove case
        Pojo pojo = new Pojo();
        pojo.setText("test1");
        Pojo pojo2 = new Pojo();
        pojo2.setText("test1");
        qdbInstance.put("test_nominal", pojo);
        qdbInstance.removeIf("test_nominal", pojo2);
        try {
            qdbInstance.get("test_nominal");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.2 : nominal case -> do not remove case
        pojo = null;
        pojo2 = null;
        pojo = new Pojo();
        pojo.setText("test1");
        pojo2 = new Pojo();
        pojo2.setText("test2");
        qdbInstance.put("test_nominal", pojo);
        try {
            qdbInstance.removeIf("test_nominal", pojo2);
            fail("No exception here.");
        } catch (Exception e) {
            Pojo pojoResult = qdbInstance.get("test_nominal");
            assertTrue(pojoResult.getText().equals(pojo.getText()));
        }
        
        // Test 3 : wrong alias
        try {
            qdbInstance.removeIf("alias_doesnt_exist", "test");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup
        qdbInstance.remove("test_nominal");
    }

    /**
     * Test of update method, of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testUpdate() throws QuasardbException {
        // Test 2.1 : wrong parameter
        try {
            qdbInstance.update("test_update_1", null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
       
        // Test 2.2 : wrong parameter
        try {
            qdbInstance.update(null, "test_update_1");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.3 : wrong parameter
        try {
            qdbInstance.update(null, null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2.1 : nominal case - simple object
        String test = "Voici un super test";
        qdbInstance.put("test_update_1", test);
        String result = qdbInstance.get("test_update_1");
        assertTrue(test.equals(result));
        
        // Test 2.2 : nominal case - pojo
        Pojo pojo = new Pojo();
        qdbInstance.update("test_update_2", pojo);
        Pojo pojoresult = qdbInstance.get("test_update_2");
        assertTrue(pojo.getText().equals(pojoresult.getText()));
          
        // Test 3 : update wrong key
        qdbInstance.update("test_update_3", "wrong key");
        String resultat = qdbInstance.get("test_update_3");
        assertTrue(resultat.equals("wrong key"));
        
        // Cleanup Qdb
        qdbInstance.remove("test_update_1");
        qdbInstance.remove("test_update_2");
        qdbInstance.remove("test_update_3");
    }
    
    /**
     * Test some errors of class Quasardb.
     * @throws QuasardbException 
     */
    @Test
    public void testErrors() throws QuasardbException {
        // Testing timeout
        Map<String,String> config = new HashMap<String,String>();
        config.put("name", "testerror");
        config.put("host", "unknown_host");
        config.put("port", PORT);     
        try {
            new Quasardb(config);
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
            assertTrue(e.getMessage().equalsIgnoreCase("Host provided was not found."));
        }
        
        // Testing empty error
        QuasardbException exception = new QuasardbException();
        assertNull("Exception must be null", exception.getMessage());
    }  
    
    @Test
    public void testGetCurrentNodeConfig() throws QuasardbException {
        try {
            NodeConfig test = null;
            test = qdbInstance.getCurrentNodeConfig();
            assertFalse(test == null);
            assertTrue(test.getLocal().getNetwork().getListen_on().equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    } 
    
    @Test
    public void testGetCurrentNodeStatus() throws QuasardbException {
        try {
            NodeStatus test = null;
            test = qdbInstance.getCurrentNodeStatus();
            assertFalse(test == null);
            assertTrue(test.getListening_addresses()[0].equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    } 
    
    @Test
    public void testGetCurrentNodeTopology() throws QuasardbException {
        try {
            NodeTopology test = null;
            test = qdbInstance.getCurrentNodeTopology();
            assertFalse(test == null);
            assertTrue(test.getCenter().getEndpoint().equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    } 
    
    @Test
    public void testGetNodeConfig() throws QuasardbException {
        try {
            NodeConfig test = null;
            test = qdbInstance.getNodeConfig("127.0.0.1", 2836);
            assertFalse(test == null);
            assertTrue(test.getLocal().getNetwork().getListen_on().equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    } 
    
    @Test
    public void testGetNodeStatus() throws QuasardbException {
        try {
            NodeStatus test = null;
            test = qdbInstance.getNodeStatus("127.0.0.1", 2836);
            assertFalse(test == null);
            assertTrue(test.getListening_addresses()[0].equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    } 
    
    @Test
    public void testGetNodeTopology() throws QuasardbException {
        try {
            NodeTopology test = null;
            test = qdbInstance.getNodeTopology("127.0.0.1", 2836);
            assertFalse(test == null);
            assertTrue(test.getCenter().getEndpoint().equalsIgnoreCase("127.0.0.1:2836"));
        } catch (Exception e) {
            fail("No exception allowed.");
        }
    }
    
    @Test
    public void testIterator() throws QuasardbException {
    	// Test 1 : iterate on 100 entries
    	try {
	    	for (int i = 0; i < 100; i++) {
	    		Pojo p = new Pojo();
	    		p.setText("test iterator " + i);
	    		qdbInstance.put("test_iterator_" + i, p);
	    	}
	    	int i = 0;
	    	for (QuasardbEntry<?> qdbe : qdbInstance) {
	    		assertTrue(qdbInstance.get(qdbe.getAlias()).equals(qdbe.getValue()));
	    		i++;
	    	}
	    	assertTrue(i == 1000);
    	} catch (Exception e) {
            fail("No exception allowed.");
        }
    	
    	// Test 2 : iterate with no entries
    	qdbInstance.removeAll();
    	assertFalse(qdbInstance.iterator().hasNext());
    	
    	// Test 2 : iterate on 1000 entries
    	try {
	    	for (int i = 0; i < 1000; i++) {
	    		Pojo p = new Pojo();
	    		p.setAbc(i);
	    		p.setText("test iterator " + i);
	    		qdbInstance.put("test_iterator_" + i, p);
	    	}
	    	int i = 0;
	    	for (QuasardbEntry<?> qdbe : qdbInstance) {
	    		assertTrue(qdbe.getAlias() != null);
	    		assertTrue(qdbe.getValue() != null);
	    		assertTrue(qdbe.getValue() instanceof Pojo);
	    		i++;
	    	}
	    	assertTrue(i == 1000);
    	} catch (Exception e) {
            fail("No exception allowed.");
        }
    	qdbInstance.removeAll();
    }
    
    @Test
    public void testGetDefaultExpiryTimeInSeconds() throws QuasardbException {
    	assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    }
    
    @Test
    public void testSetDefaultExpiryTimeInSeconds() throws QuasardbException {
    	long expiryTime=20L;
    	
    	// Test 1 : set default expiry time to 20s
    	qdbInstance.setDefaultExpiryTimeInSeconds(expiryTime);
    	assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);
    	
    	// Test 2 : set default expiry time to eternal
    	qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    	assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    	
    	// Test 3 : set default expiry time to eternal
    	qdbInstance.setDefaultExpiryTimeInSeconds(-1);
    	assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == 0L);
    	
    	// Test 4 : test default expiry time with config
    	Map<String,String> config2 = new HashMap<String,String>();
    	config2.put("name", "test");
    	config2.put("host", HOST);
    	config2.put("port", PORT);
    	config2.put("expiry", "20");
    	qdbInstance = new Quasardb(config2);
        try {
            qdbInstance.connect();
        } catch (QuasardbException e) {
            e.printStackTrace();
        }
        assertTrue(qdbInstance.getDefaultExpiryTimeInSeconds() == expiryTime);
        
        // Clean up
        config2 = null;
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    }
    
    @Test
    public void testPutWithExpiryTime() throws QuasardbException {
    	long expiry = 1L;
    	
    	// Test 1 : nominal case
        String test = "Voici un super test";
        qdbInstance.put("test_put_expiry_1", test, expiry);
        String result = qdbInstance.get("test_put_expiry_1");
        assertTrue(test.equals(result));
        try {
			Thread.sleep(expiry * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        try {
            qdbInstance.get("test_put_expiry_1");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : negative parameter
        qdbInstance.put("test_put_expiry_1", test, -1);
        try {
			Thread.sleep(expiry * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        assertTrue(((String) qdbInstance.get("test_put_expiry_1")).equalsIgnoreCase(test));
          
        // Test 3 : expiry time = current time + 2 seconds
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
    	cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    	long calculatedExpiry = cal.getTimeInMillis()/1000;
        qdbInstance.put("test_put_expiry_2", test, 2L);
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_put_expiry_2") == calculatedExpiry);
        try {
			Thread.sleep(2L * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        try {
            qdbInstance.get("test_put_expiry_2");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 4 : put entries with different expiry times
        qdbInstance.setDefaultExpiryTimeInSeconds(0L);
        qdbInstance.put("test_put_expiry_2", test);
        qdbInstance.put("test_put_expiry_3", test, expiry);
        qdbInstance.put("test_put_expiry_4", test, expiry + expiry);
        try {
			Thread.sleep((expiry + (expiry / 2)) * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        assertTrue(((String) qdbInstance.get("test_put_expiry_1")).equalsIgnoreCase(test));
        assertTrue(((String) qdbInstance.get("test_put_expiry_2")).equalsIgnoreCase(test));
        try {
            qdbInstance.get("test_put_expiry_3");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        assertTrue(((String) qdbInstance.get("test_put_expiry_4")).equalsIgnoreCase(test));
        try {
			Thread.sleep(expiry * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        try {
            qdbInstance.get("test_put_expiry_4");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup Qdb
        qdbInstance.remove("test_put_expiry_1");
        qdbInstance.remove("test_put_expiry_2");
    }
    
    @Test
    public void testSetExpiryTimeInSeconds() throws QuasardbException {
    	qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    	qdbInstance.removeAll();
    	
    	// Test 1 : nominal case
        String test = "Voici un super test";
        qdbInstance.put("test_expiry_1", test);
        try {
			Thread.sleep(1L * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        assertTrue(((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", 1L);
        try {
			Thread.sleep(1L * 1000 + 500);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        try {
            qdbInstance.get("test_expiry_1");
            fail("An exception must be thrown because alias expired.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 2 : negative param
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
     	cal.setTimeZone(TimeZone.getTimeZone("UTC"));
     	long calculatedExpiry = cal.getTimeInMillis()/1000;    	
        qdbInstance.put("test_expiry_1", test, 2L);
        try {
			Thread.sleep(1L * 1000);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        assertTrue(((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_expiry_1") == calculatedExpiry);
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", -1);
        try {
			Thread.sleep(1L * 1000 + 500);
		} catch (InterruptedException e1) {
			fail("No exception allowed.");
		}
        assertTrue(((String) qdbInstance.get("test_expiry_1")).equalsIgnoreCase(test));
        GregorianCalendar cal2 = new GregorianCalendar();
        cal2.add(Calendar.YEAR, 10);
        cal2.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_expiry_1") > (cal2.getTimeInMillis()/1000));
        
        // Test 3 : invalid alias
        try {
            qdbInstance.setExpiryTimeInSeconds("wrong_alias", 1L);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 4 : null alias
        try {
        	qdbInstance.setExpiryTimeInSeconds(null, 1L);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup Qdb
        qdbInstance.remove("test_expiry_1");
    }
    
    @Test
    public void testGetExpiryTimeInSeconds() throws QuasardbException {
    	qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    	qdbInstance.removeAll();
    	String test = "Voici un super test";
    	
    	// Test 1 : nominal case
    	GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, 2);
     	cal.setTimeZone(TimeZone.getTimeZone("UTC"));
     	long calculatedExpiry = cal.getTimeInMillis()/1000;    	
    	qdbInstance.put("test_expiry_1", test, 2L);
        try {
 			Thread.sleep(1L * 1000);
 		} catch (InterruptedException e1) {
 			fail("No exception allowed.");
 		}        
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_expiry_1") == calculatedExpiry);
        qdbInstance.setExpiryTimeInSeconds("test_expiry_1", -1);
        try {
 			Thread.sleep(1L * 1000 + 500);
 		} catch (InterruptedException e1) {
 			fail("No exception allowed.");
 		}
        GregorianCalendar cal2 = new GregorianCalendar();
        cal2.add(Calendar.YEAR, 10);
        cal2.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertTrue(qdbInstance.getExpiryTimeInSeconds("test_expiry_1") > (cal2.getTimeInMillis()/1000));
        
        // Test 2 : invalid alias
        try {
            qdbInstance.getExpiryTimeInSeconds("wrong_alias");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 3 : null alias
        try {
        	qdbInstance.getExpiryTimeInSeconds(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Test 4 : empty alias
        try {
        	qdbInstance.getExpiryTimeInSeconds("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
        
        // Cleanup Qdb
        qdbInstance.remove("test_expiry_1");
    }
    
    @Test
    public void testSetExpiryTimeAt() throws QuasardbException {
    	qdbInstance.setDefaultExpiryTimeInSeconds(0L);
    	qdbInstance.removeAll();
    	String test = "Voici un super test";
    	qdbInstance.put("test_expiry_1", test);
    	long time = System.currentTimeMillis() + (1000 * 60 * 60);
    	Date expiryDate = new Date(time);
    	    	
    	// Test 1 : null param
    	try {
        	qdbInstance.setExpiryTimeAt(null, expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 2 : null date
    	try {
        	qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 3 : empty alias
    	try {
        	qdbInstance.setExpiryTimeAt("", expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 4 : wrong alias
    	try {
        	qdbInstance.setExpiryTimeAt("wrong_alias", expiryDate);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 5 : nominal case
    	qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
    	assertTrue((qdbInstance.getExpiryTimeInSeconds("test_expiry_1") * 1000) == time);
    	
    	// Cleanup Qdb
    	qdbInstance.removeAll();
    }
    
    @Test
    public void testGetExpiryTimeInDate() throws QuasardbException {
    	qdbInstance.removeAll();
    	
    	// Test 1 : null param
    	try {
        	qdbInstance.getExpiryTimeInDate(null);
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 2 : empty alias
    	try {
        	qdbInstance.getExpiryTimeInDate("");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 2 : wrong alias
    	try {
        	qdbInstance.getExpiryTimeInDate("wrong_alias");
            fail("An exception must be thrown.");
        } catch (Exception e) {
            assertTrue(e instanceof QuasardbException);
        }
    	
    	// Test 4 : nominal case
    	long time = System.currentTimeMillis() + (1000 * 60 * 60);
    	Date expiryDate = new Date(time);
    	qdbInstance.setExpiryTimeAt("test_expiry_1", expiryDate);
    	assertTrue(qdbInstance.getExpiryTimeInDate("test_expiry_1").equals(expiryDate));
    	
    	// Cleanup Qdb
    	qdbInstance.removeAll();
    }
}
