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
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
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

package com.b14.qdb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.b14.qdb.data.Address;
import com.b14.qdb.data.Customer;
import com.b14.qdb.data.EntityWithCollections;
import com.b14.qdb.data.Gender;
import com.b14.qdb.data.LineItemKey;
import com.b14.qdb.data.Pojo;
import com.b14.qdb.data.PojoSerializable;
import com.b14.qdb.data.Skill;
import com.b14.qdb.data.Subscription;
import com.b14.qdb.data.SubscriptionType;
import com.b14.qdb.data.User;

public class QuasardbStabilityTest {
    private static final QuasardbConfig config = new QuasardbConfig();
    private static final Quasardb qdb = new Quasardb();
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ArrayList toList (Object... items) {
        ArrayList list = new ArrayList();
        for (Object item : items) {
            list.add(item);
        }
        return list;
    }
    
    public QuasardbStabilityTest() {
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        QuasardbNode node = new QuasardbNode(QuasardbTest.HOST, QuasardbTest.PORT);
        config.addNode(node);
        try {
            qdb.setConfig(config); 
            qdb.connect();
        } catch (QuasardbException e) {
            System.err.println("Could not initialize Quasardb connection pool for Loader: " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try {
            if (qdb != null) {
                qdb.close();
            }
        } catch (QuasardbException e) {
        }
    }
    
    @Before
    public void init() throws Exception {
    }
    
    @After
    public void cleanUp() {
    }
    
    private void cleanKeyIfExist(String key) {
        // Clean up stored value
        try {
            qdb.remove(key);
        } catch (QuasardbException e) {
        }
    }
    
    @Test
    public void stringTest() {
        String key = "test_1";
        String value = "Here is my first test.";
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            
            value = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\rabcdefghijklmnopqrstuvwxyz\n1234567890\t\"!`?'.,;:()[]{}<>|/@\\^$-%+=#_&~*";
            qdb.put(key, value);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value, qdb.get(key)));
            qdb.remove(key);
           
            value = "abcdef\u00E1\u00E9\u00ED\u00F3\u00FA\u1234";
            qdb.put(key, value);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value, qdb.get(key)));
            qdb.remove(key);
            
            StringBuffer value2 = new StringBuffer("<stringbuffer>with some content \n& some lines...</stringbuffer>");
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value2, qdb.get(key)));
            qdb.remove(key);
            
            StringBuilder value3 = new StringBuilder("<stringbuilder>with some content \n& some lines...</stringbuilder>");
            qdb.put(key, value3);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value3, qdb.get(key)));
            qdb.remove(key);
            
            Character[] value4 = new Character[] { new Character('t'), new Character('e'), new Character('s'), new Character('t')};
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            qdb.remove(key);
            
            char[] value5 = "123456789".toCharArray();
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));            
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            qdb.remove(key);
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        } 
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void pojoTest() {
        String key = "test_1";
        Pojo value = new Pojo();
        value.setText("POJO");
        value.setAbc(312f);
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            
            PojoSerializable value2 = new PojoSerializable();
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value2, qdb.get(key)));
            
            qdb.remove(key);
            LineItemKey value3 = new LineItemKey(new Integer(12345), 12345);
            qdb.put(key, value3);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value3, qdb.get(key)));
            
            qdb.remove(key);
            EntityWithCollections value4 = new EntityWithCollections();
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            Customer value5 = new Customer("1234","Olivier", "BONTEMPS");
            value5.setAddresses(toList(new Address("14", "avenue de l'opera", "PARIS", "75001", "FRANCE"), new Address("119", "rue caulaincourt", "PARIS", "75018", "FRANCE"))); 
            value5.setSubscriptions(toList(new Subscription("Le Monde", SubscriptionType.NEWS_PAPER), new Subscription("Joystick", SubscriptionType.MAGAZINE), new Subscription("20 Minutes", SubscriptionType.JOURNAL)));
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void skillTest() {
        String key = "test_1";
        Skill value = new Skill("testeur");
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void userTest() {
        String key = "test_1";
        User value = new User("login", "password", "User de test");
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void dateTest() {
        String key = "test_1";
        Date value = new Date(System.currentTimeMillis());
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            GregorianCalendar value2 = new GregorianCalendar();
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value2, qdb.get(key)));
            
            qdb.remove(key);
            java.sql.Date value3 = new java.sql.Date(1234567);
            qdb.put(key, value3);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value3, qdb.get(key)));
            
            qdb.remove(key);
            java.sql.Time value4 = new java.sql.Time(1234567);
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            java.sql.Timestamp value5 = new java.sql.Timestamp(1234567);
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            Calendar value6 = Calendar.getInstance(Locale.ENGLISH);
            qdb.put(key, value6);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value6, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void numbersTest() {
        String key = "test_1";
        Long value = new Long(Long.MAX_VALUE);
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            Integer value2 = new Integer(333333);
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value2, qdb.get(key)));
            
            qdb.remove(key);
            Float value3 = new Float(34455.33453f);
            qdb.put(key, value3);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value3, qdb.get(key)));
            
            qdb.remove(key);
            Double value4 = new Double(12345);
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            value4 = Double.MAX_VALUE;
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            Integer value5 = Integer.MAX_VALUE;
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            BigInteger value6 = BigInteger.valueOf(1270507903945L);
            qdb.put(key, value6);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value6, qdb.get(key)));
            
            qdb.remove(key);
            BigDecimal value7 = BigDecimal.valueOf(125678990L, 2);
            qdb.put(key, value7);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value7, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void listsTest() {
        String key = "test_1";
        Object[] value = new Object[] {new String[] {"11", "2222", null, "4"}, new int[] {1, 2, 3, 4}, new int[][] { {1, 2}, {100, 4}}};
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            value = new Object[] {new Pojo(), new User("l","p", "test"), new Skill("testing skill") };
            qdb.put(key, value);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.remove(key);
            String[] value2 = new String[] {"11", "2222", "3", "4"};
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value2, qdb.get(key)));
           
            qdb.remove(key);
            @SuppressWarnings({ "rawtypes", "unchecked" })
            LinkedList value3 = new LinkedList(toList("1", "2", toList("3","4")));
            qdb.put(key, value3);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value3, qdb.get(key)));
            
            qdb.remove(key);
            List<String> value4 = Collections.singletonList("test-SingletonList");
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            Set<String> value5 = Collections.singleton("test-SingletonSet");
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            Map<String,String> value6 = Collections.singletonMap("test-SingletonMap", "test");
            qdb.put(key, value6);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value6, qdb.get(key)));
            
            qdb.remove(key);
            EnumSet<Gender> value7 = EnumSet.allOf(Gender.class);
            qdb.put(key, value7);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value7, qdb.get(key)));
            
            qdb.remove(key);
            value4 = Collections.unmodifiableList(new ArrayList<String>(Arrays.asList("foo", "bar")));
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            value5 = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("foo", "bar")));
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            value4 = Collections.synchronizedList(new ArrayList<String>(Arrays.asList("foo", "bar")));
            qdb.put(key, value4);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value4, qdb.get(key)));
            
            qdb.remove(key);
            value5 = Collections.synchronizedSet( new HashSet<String>(Arrays.asList("foo", "bar")));
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            List<?> value8 = Arrays.asList();
            qdb.put(key, value8);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value8, qdb.get(key)));
            
            qdb.remove(key);
            value8 = Arrays.asList(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            qdb.put(key, value8);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value8, qdb.get(key)));
           
            qdb.remove(key);
            value8 = Arrays.asList( "foo", "bar" );
            qdb.put(key, value8);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value8, qdb.get(key)));
            
            qdb.remove(key);
            Map<String, String> hashmap = new HashMap<String, String>();
            hashmap.put("foo", "bar");
            Map<String, String> value9 = hashmap;
            qdb.put(key, value9);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value9, qdb.get(key)));
            
            qdb.remove(key);
            value9 = Collections.synchronizedMap(hashmap);
            qdb.put(key, value9);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value9, qdb.get(key)));
            
            qdb.remove(key);
            value9 = Collections.unmodifiableMap(hashmap);
            qdb.put(key, value9);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value9, qdb.get(key)));
            
            qdb.remove(key);
            value8 = Collections.<String>emptyList();
            qdb.put(key, value8);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value8, qdb.get(key)));
            
            qdb.remove(key);
            value5 = Collections.<String>emptySet();
            qdb.put(key, value5);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value5, qdb.get(key)));
            
            qdb.remove(key);
            value9 = Collections.<String, String>emptyMap(); 
            qdb.put(key, value9);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value9, qdb.get(key)));

            qdb.remove(key);
            @SuppressWarnings({ "rawtypes", "unchecked" })
            CopyOnWriteArrayList value10 = new CopyOnWriteArrayList(toList("1", "2", toList("3")));
            qdb.put(key, value10);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value10, qdb.get(key)));
            
            qdb.remove(key);
            ArrayList<Double> value11 = new ArrayList<Double>();
            value11.add(148.73);
            value11.add(162.47);
            value11.add(175.22);
            value11.add(187.48);
            value11.add(203.45);
            value11.add(218.26);
            value11.add(233.36);
            value11.add(249.25);
            value11.add(263.92);
            value11.add(277.45);
            value11.add(290.25);
            value11.add(303.36);
            value11.add(313.72);
            value11.add(324.59);
            value11.add(335.92);
            value11.add(360.95);
            value11.add(375.18);
            value11.add(396.76);
            value11.add(407.99);
            value11.add(419.17);
            value11.add(430.73);
            value11.add(441.81);
            value11.add(454.75);
            value11.add(462.60);
            value11.add(472.01);
            value11.add(481.23);
            value11.add(489.55);
            value11.add(496.98);
            value11.add(503.44);
            value11.add(509.83);
            qdb.put(key, value11);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value11, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void booleanTest() {
        String key = "test_1";
        Boolean value = new Boolean(true);
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            value = Boolean.FALSE;
            qdb.put(key, value);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void currencyTest() {
        String key = "test_1";
        Currency value = Currency.getInstance("EUR");
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
    
    @Test
    public void byteTest() {
        String key = "test_1";
        Byte value = new Byte("b".getBytes()[0]);
        
        cleanKeyIfExist(key);
        
        try {
            // Insert the provided value at provided key
            qdb.put(key, value);
            
            // Check if a value has been stored at key
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value, qdb.get(key)));
            
            qdb.update(key, new String("UPDATED"));
            assertNotNull(qdb.get(key));
            assertTrue(qdb.get(key).toString().equals("UPDATED"));
            
            qdb.remove(key);
            byte[] value2 = "123456789".getBytes();
            qdb.put(key, value2);
            assertNotNull(qdb.get(key));
            assertTrue(checkIfEquals(value2, qdb.get(key)));
        } catch (QuasardbException e) {
            fail("Cannot insert key[" + key + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up stored value
            try {
                qdb.remove(key);
            } catch (QuasardbException e) {
            }
        }
    }
       
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> boolean checkIfEquals(final T obj1, final T obj2) {
        if (obj1 instanceof Object[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && (Arrays.deepEquals((T[]) obj1, (T[]) obj2)));
        } else if (obj1 instanceof boolean[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((boolean[]) obj1).length == ((boolean[]) obj2).length);
        } else if (obj1 instanceof byte[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((byte[]) obj1).length == ((byte[]) obj2).length);
        } else if (obj1 instanceof char[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((char[]) obj1).length == ((char[]) obj2).length);
        } else if (obj1 instanceof short[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((short[]) obj1).length == ((short[]) obj2).length);
        } else if (obj1 instanceof int[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((int[]) obj1).length == ((int[]) obj2).length);
        } else if (obj1 instanceof long[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((long[]) obj1).length == ((long[]) obj2).length);
        } else if (obj1 instanceof float[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((float[]) obj1).length == ((float[]) obj2).length);
        } else if (obj1 instanceof double[]) {
            return (obj1 == obj2 || obj1 != null && obj2 != null && ((double[]) obj1).length == ((double[]) obj2).length);
        } else if (obj1.getClass().getName().contains("String")) {
            return obj1.toString().equals(obj2.toString());
        } else if (obj1 instanceof Comparable) {
            if (((Comparable) obj1).compareTo(obj2) != 0) {
                return obj1.toString().equals(obj2.toString());
            } else  {
                return true;
            }
        } else if (obj1 instanceof Number) {
            return obj1.toString().equals(obj2.toString());
        } else if (obj1 instanceof Collection<?>) {
            if (!((Collection<?>) obj1).containsAll(((Collection<?>) obj2))) {
                Iterator<?> it = ((Collection<?>) obj1).iterator();
                Iterator<?> it2 = ((Collection<?>) obj2).iterator();
                while(it.hasNext() && it2.hasNext()) {
                    return checkIfEquals(it.next(), it2.next());
                }
                return false;
            } else {
                return true;
            }
        } else {
            return obj1.equals(obj2);
        }
    }
}
