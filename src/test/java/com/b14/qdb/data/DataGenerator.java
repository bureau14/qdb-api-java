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

package com.b14.qdb.data;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import net.sf.cglib.core.DefaultNamingPolicy;
import net.sf.cglib.core.Predicate;
import net.sf.cglib.proxy.Enhancer;

import org.databene.benerator.util.SimpleGenerator;

@SuppressWarnings("unchecked")
public class DataGenerator extends SimpleGenerator<Object[]> {

    private static final ArrayList<Double> doubleList = new ArrayList<Double>();
    private static final int[][][] triple = new int[10][20][30];
    private static final Address addressb14 = new Address("14", "avenue de l'opera", "PARIS", "75001", "FRANCE");
    private static final Address address = new Address("119", "rue caulaincourt", "PARIS", "75018", "FRANCE");
    private static final Subscription news = new Subscription("Le Monde", SubscriptionType.NEWS_PAPER);
    private static final Subscription mag = new Subscription("Joystick", SubscriptionType.MAGAZINE);
    private static final Subscription jour = new Subscription("20 Minutes", SubscriptionType.JOURNAL);
    private static final Customer customer = new Customer("1234","Olivier", "BONTEMPS");
    private static final EnumMap<Gender, String> enummap = new EnumMap<Gender, String>( Gender.class );
    private static final Map<Double, String> linkedhashmap = new LinkedHashMap<Double, String>();
    private static final HashMap<String, String> hashmap = new HashMap<String, String>();
    private static final Person p1 = new Person("john doe", Gender.MALE, 42);
    private static final Person p2 = new Person("joahnna doe", Gender.FEMALE, 42);
    private static ClassToProxy proxy1 = createProxy(new ClassToProxy());
    private static Map<String, String> proxy2 = createProxy(new HashMap<String, String>());
    private static final Date date = new Date();
    private static final byte[] bytes = new byte[] { (byte) 0xFF, (byte) 0xDA, (byte) 0xB6, (byte) 0x91, (byte) 0x6D, (byte) 0x48, (byte) 0x24, (byte) 0x00 };
    private static final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    private static final BitSet bitSet = new BitSet(10);
    
    @SuppressWarnings("rawtypes")
    public static final Object[][] data = new Object[][] {
        { "test1", "Here is my first test." }, 
        { "test2", new Pojo() }, 
        { "test3", new User("login", "password", "User de test") }, 
        { "test4", new Skill("testeur") },
        { "test5", new Long(Long.MAX_VALUE)}, 
        { "test6", "ABCDEFGHIJKLMNOPQRSTUVWXYZ\rabcdefghijklmnopqrstuvwxyz\n1234567890\t\"!`?'.,;:()[]{}<>|/@\\^$-%+=#_&~*" }, 
        { "test7", "abcdef\u00E1\u00E9\u00ED\u00F3\u00FA\u1234" }, 
        { "test8", new LinkedList(toList("1", "2", toList("3","4"))) },
        { "test9", date },
        { "test10", new String[] {"11", "2222", "3", "4"} }, 
        { "test11", new Object[] {new String[] {"11", "2222", null, "4"}, new int[] {1, 2, 3, 4}, new int[][] { {1, 2}, {100, 4}}} }, 
        { "test12", new Integer(333333) }, 
        { "test13", new Float(34455.33453f) }, 
        { "test14", Double.MAX_VALUE }, 
        { "test15", Float.MAX_VALUE }, 
        { "test16", Integer.MAX_VALUE }, 
        { "test17", (byte)125 }, 
        { "test18", BigInteger.valueOf(1270507903945L) }, 
        { "test19", BigDecimal.valueOf(125678990L, 2) },
        { "test20", new Boolean(true) }, 
        { "test21", Boolean.FALSE }, 
        { "test22", new Object[] {new Pojo(), new User("l","p", "test"), new Skill("testing skill") } }, 
        { "test23", new CopyOnWriteArrayList(toList("1", "2", toList("3"))) }, 
        { "test24", new PojoSerializable() },
        { "test25", new Double(12345) }, 
        { "test26", new Object[][] { { 1 }, { 2 }, { 3 }, { 4 } } }, 
        { "test27", UUID.randomUUID() }, 
        { "test28", new Float[] {2.0f, 4.0f} },
        { "test29", new Long[] {4l, 5l} }, 
        { "test30", new boolean[] {true, false} },
        { "test31", doubleList },
        { "test32", new Byte(Byte.MAX_VALUE) },
        { "test33", new Character[] { new Character('t'), new Character('e'), new Character('s'), new Character('t')} },
        { "test34", new GregorianCalendar() },
        { "test35", new java.sql.Date(1234567) },
        { "test36", new java.sql.Time(1234567) },
        { "test37", new java.sql.Timestamp(1234567) },
        { "test38", new LineItemKey(new Integer(12345), 12345) },
        { "test39", customer },
        { "test40", Collections.singletonList("test-SingletonList") },
        { "test41", Collections.singleton("test-SingletonSet") },
        { "test42", Collections.singletonMap("test-SingletonMap", "test") },
        { "test43", EnumSet.allOf(Gender.class) },
        { "test44", bitSet },
        { "test45", linkedhashmap },
        { "test46", Calendar.getInstance( Locale.ENGLISH ) },
        { "test47", new StringBuffer("<stringbuffer>with some content \n& some lines...</stringbuffer>") },
        { "test48", new StringBuilder("<stringbuilder>with some content \n& some lines...</stringbuilder>") },
        { "test49", Currency.getInstance("EUR") },
        { "test50", Collections.unmodifiableList( new ArrayList<String>(Arrays.asList("foo", "bar"))) },
        { "test51", Collections.unmodifiableSet( new HashSet<String>(Arrays.asList("foo", "bar"))) },
        { "test52", Collections.synchronizedList( new ArrayList<String>(Arrays.asList("foo", "bar"))) },
        { "test53", Collections.synchronizedSet( new HashSet<String>(Arrays.asList("foo", "bar"))) },
        { "test54", hashmap },
        { "test55", Collections.synchronizedMap(hashmap) },
        { "test56", Collections.unmodifiableMap(hashmap) },
        { "test57", Collections.<String>emptyList() },
        { "test58", Collections.<String>emptySet() },
        { "test59", Collections.<String, String>emptyMap() },
        { "test60", Arrays.asList() },
        { "test61", Arrays.asList(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }) },
        { "test62", Arrays.asList( "foo", "bar" ) },
        { "test63", String.class },
        { "test64", new AtomicLong(42) },
        { "test65", "123456789".getBytes() },
        { "test66", "123456789".toCharArray() },
        { "test67", new Container() },
        { "test68", new File("/tmp") },
        { "test79", new MyContainer() },
        { "test70", new EntityWithCollections() },
        { "test71", new Byte("b".getBytes()[0]) },
        { "test72", buffer },
        { "test73", proxy1 }
    };
    
    static {
        // Date
        date.setTime(System.currentTimeMillis());
        
        // ArrayList
        doubleList.add(148.73);
        doubleList.add(162.47);
        doubleList.add(175.22);
        doubleList.add(187.48);
        doubleList.add(203.45);
        doubleList.add(218.26);
        doubleList.add(233.36);
        doubleList.add(249.25);
        doubleList.add(263.92);
        doubleList.add(277.45);
        doubleList.add(290.25);
        doubleList.add(303.36);
        doubleList.add(313.72);
        doubleList.add(324.59);
        doubleList.add(335.92);
        doubleList.add(360.95);
        doubleList.add(375.18);
        doubleList.add(396.76);
        doubleList.add(407.99);
        doubleList.add(419.17);
        doubleList.add(430.73);
        doubleList.add(441.81);
        doubleList.add(454.75);
        doubleList.add(462.60);
        doubleList.add(472.01);
        doubleList.add(481.23);
        doubleList.add(489.55);
        doubleList.add(496.98);
        doubleList.add(503.44);
        doubleList.add(509.83);
        
        // Triple dimension        
        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < 20; ++j) {
                for (int k = 0; k < 30; ++k) {
                    triple[i][j][k] = (int) (Math.random() * 100);
                }
            }
        }
        
        // Customer
        customer.setAddresses(toList(addressb14, address));
        customer.setSubscriptions(toList(news, mag, jour));
        
        // EnumMap
        enummap.put(Gender.FEMALE, "female");
        
        // LinkedHashMap
        // use doubles as e.g. integers hash to the value...
        for( int i = 0; i < 10; i++ ) {
            linkedhashmap.put(Double.valueOf(String.valueOf(i) + "." + Math.abs(i)), "value: " + i );
        }
        
        // HashMap 
        hashmap.put("foo", "bar");
        
        // Cyclic dependency
        p1.addFriend(p2);
        p2.addFriend(p1);
        
        // CGLib tests
        proxy1.setValue("foo");
        proxy2.put("foo", "bar");
    }
    
    @SuppressWarnings("rawtypes")
    private static ArrayList toList (Object... items) {
        ArrayList list = new ArrayList();
        for (Object item : items) {
            list.add(item);
        }
        return list;
    }
    
    private static <T> T createProxy(T obj) {
        Enhancer e = new Enhancer();
        e.setInterfaces( new Class[] { Serializable.class } );
        Class<? extends Object> class1 = obj.getClass();
        e.setSuperclass( class1 );
        e.setCallback( new DelegatingHandler( obj ) );
        e.setNamingPolicy( new DefaultNamingPolicy() {
            @Override
            public String getClassName(String prefix, String source, Object key, Predicate names) {
                return super.getClassName( "MSM_" + prefix, source, key, names );
            }
        } );
        return (T) e.create();
    }

    int count = 0;
    
    public Object[] generate() {
        if (count >= data.length) return null;
        
        Object[] result = data[count];
        count++;
        return result;
    }

    public Class<Object[]> getGeneratedType() {
        return Object[].class;
    }
}
