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
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
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
package com.b14.qdb.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.b14.qdb.QuasardbNode;
import com.b14.qdb.hadoop.mahout.QuasardbPreference;

/**
 * A unit test case for {@link QuasardbInputSplit} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbInputSplitTest {
    @Mock 
    private DataInput input;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link QuasardbInputSplit#getLength()}.
     */
    @Test
    public void testGetLengthWithNoInputs() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        assertEquals("No inputs means getLength should return 0.", 0L, qdbInputSplit.getLength());
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getLength()}.
     */
    @Test
    public void testGetLengthWithNullInputs() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        assertEquals("No inputs means getLength should return 0.", 0L, qdbInputSplit.getLength());
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getLength()}.
     */
    @Test
    public void testGetLength() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), null);
        assertEquals("getLength should return 3.", 3L, qdbInputSplit.getLength());
    }

    /**
     * Test method for {@link QuasardbInputSplit#QuasardbInputSplit(java.util.List, QuasardbNode[])}.
     */
    @Test
    public void testQuasardbInputSplitWithNullParams() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        assertEquals("No inputs means getLength should return 0.", 0L, qdbInputSplit.getLength());
        assertEquals("No nodes means getLocations should return empty array.", 0, qdbInputSplit.getLocations().length);
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#QuasardbInputSplit(java.util.List, QuasardbNode[])}.
     */
    @Test
    public void testQuasardbInputSplitWithNullInputs() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        List<String> locationsExpected = Arrays.asList("127.0.0.1:2836", "127.0.0.1:2837", "127.0.0.1:2838");
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, qdbNodes);
        assertEquals("No inputs means getLength should return 0.", 0L, qdbInputSplit.getLength());
        assertEquals("getLocations should return 3 locations.", 3, qdbInputSplit.getQdbLocations().length);
        List<String> locationsResult = new ArrayList<String>();
        for (QuasardbNode qdbNode : qdbInputSplit.getQdbLocations()) {
            locationsResult.add(qdbNode.toString());
        }
        assertTrue("Both locations should be equals.", locationsExpected.containsAll(locationsResult));
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#QuasardbInputSplit(java.util.List, QuasardbNode[])}.
     */
    @Test
    public void testQuasardbInputSplitWithNullNodes() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), null);
        assertEquals("getLength should return 3.", 3L, qdbInputSplit.getLength());
        assertEquals("No nodes means getLocations should return empty array.", 0, qdbInputSplit.getLocations().length);
    }

    /**
     * Test method for {@link QuasardbInputSplit#getInputs()}.
     */
    @Test
    public void testGetInputsWithNoInputs() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        assertTrue("No inputs means empty inputs.", qdbInputSplit.getInputs().isEmpty());        
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getInputs()}.
     */
    @Test
    public void testGetInputs() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), null);
        assertFalse("Inputs list shouldn't be empty.", qdbInputSplit.getInputs().isEmpty());
        assertEquals("Input list size should be equals to 3.", 3, qdbInputSplit.getInputs().size());
        assertTrue("Both inputs should be equals.", Arrays.asList("key1", "key2", "key3").containsAll(qdbInputSplit.getInputs()));
    }

    /**
     * Test method for {@link QuasardbInputSplit#readFields(java.io.DataInput)}.
     */
    @Test
    public void testReadFieldsWithWrongInputFormatValues() {
        try {
            // Mock nodes and keys
            when(input.readInt()).thenReturn(1).thenReturn(1);
            when(input.readUTF()).thenReturn("foo").thenReturn("bar");
        } catch (IOException e) {}
        
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        try {
            qdbInputSplit.readFields(input);
            fail("Should raise an IOException because provided param was wrong.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#readFields(java.io.DataInput)}.
     */
    @Test
    public void testReadFieldsWithWrongPortValues() {
        try {
            // Mock nodes and keys
            when(input.readInt()).thenReturn(1).thenReturn(1);
            when(input.readUTF()).thenReturn("127.0.0.1:foo").thenReturn("bar");
        } catch (IOException e) {}
        
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        try {
            qdbInputSplit.readFields(input);
            fail("Should raise an IOException because provided port param was wrong.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#readFields(java.io.DataInput)}.
     */
    @Test
    public void testReadFieldsWithNoValues() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        try {
            DataInput inputEmpty = new DataInputStream(new FileInputStream("foo.txt"));
            qdbInputSplit.readFields(inputEmpty);
            fail("Should raise an IOException because provided params were wrong.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#readFields(java.io.DataInput)}.
     */
    @Test
    public void testReadFieldsWithNullParamMeansAnException() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        try {
            qdbInputSplit.readFields(null);
            fail("Should raise an IOException because provided param was null.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#readFields(java.io.DataInput)}.
     */
    @Test
    public void testReadFields() {
        try {
            // Mock nodes and keys
            when(input.readInt()).thenReturn(3).thenReturn(3);
            when(input.readUTF()).thenReturn("127.0.0.1:2836").thenReturn("127.0.0.1:2837").thenReturn("127.0.0.1:2838").thenReturn("key1").thenReturn("key2").thenReturn("key3");
        } catch (IOException e) {}
        
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit();
        try {
            qdbInputSplit.readFields(input);
        } catch (IOException e) {
            fail("No Exception allowed.");
        }

        List<String> locationsExpected = Arrays.asList("127.0.0.1:2836", "127.0.0.1:2837", "127.0.0.1:2838");
        List<String> keysExpected = Arrays.asList("key1", "key2", "key3");
        List<String> locationsResult = new ArrayList<String>();
        for (QuasardbNode qdbNode : qdbInputSplit.getQdbLocations()) {
            locationsResult.add(qdbNode.toString());
        }
        assertTrue("Both locations should be equals.", locationsExpected.containsAll(locationsResult));
        assertTrue("Both inputs should be equals.", keysExpected.containsAll(qdbInputSplit.getInputs()));
    }

    /**
     * Test method for {@link QuasardbInputSplit#write(java.io.DataOutput)}.
     */
    @Test
    public void testWriteWithNullParamMeansAnException() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        try {
            qdbInputSplit.write(null);
            fail("Should raise an IOException because provided param was null.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#write(java.io.DataOutput)}.
     */
    @Test
    public void testWriteWithNullNodes() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), null);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("bar_1.txt");
            qdbInputSplit.write(new DataOutputStream(fos));
            fail("Should raise an IOException because inputsplit has no nodes.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                (new File("bar_1.txt")).delete();
            } catch (Exception e) {}
        }
    }

    /**
     * Test method for {@link QuasardbInputSplit#write(java.io.DataOutput)}.
     */
    @Test
    public void testWriteWithNullInputs() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, qdbNodes);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("bar_2.txt");
            qdbInputSplit.write(new DataOutputStream(fos));
            fail("Should raise an IOException because inputsplit has no inputs.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                (new File("bar_2.txt")).delete();
            } catch (Exception e) {}
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#write(java.io.DataOutput)}.
     */
    @Test
    public void testWriteWithNullInitialization() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("bar_3.txt");
            qdbInputSplit.write(new DataOutputStream(fos));
            fail("Should raise an IOException because inputsplit has no inputs.");
        } catch (Exception e) {
            assertTrue("Exception should be an IOException", e instanceof IOException);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                (new File("bar_3.txt")).delete();
            } catch (Exception e) {}
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#write(java.io.DataOutput)}.
     */
    @Test
    public void testWrite() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), qdbNodes);
        FileOutputStream fos = null;
        FileInputStream fis = null;
        try {
            fos = new FileOutputStream("bar_4.txt");
            qdbInputSplit.write(new DataOutputStream(fos));
            fos.close();
            qdbInputSplit = null;
            
            fis = new FileInputStream("bar_4.txt");
            qdbInputSplit = new QuasardbInputSplit();
            qdbInputSplit.readFields(new DataInputStream(fis));
            fis.close();
            
            List<String> locationsExpected = Arrays.asList("127.0.0.1:2836", "127.0.0.1:2837", "127.0.0.1:2838");
            List<String> keysExpected = Arrays.asList("key1", "key2", "key3");
            List<String> locationsResult = new ArrayList<String>();
            for (QuasardbNode qdbNode : qdbInputSplit.getQdbLocations()) {
                locationsResult.add(qdbNode.toString());
            }
            assertTrue("Both locations should be equals.", locationsExpected.containsAll(locationsResult));
            assertTrue("Both inputs should be equals.", keysExpected.containsAll(qdbInputSplit.getInputs()));
        } catch (Exception e) {
            fail("Shouldn't raise an IOException because initialization was OK.");
        } finally {
            try {
                (new File("bar_4.txt")).delete();
            } catch (Exception e) {}
        }
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getLocations()}.
     */
    @Test
    public void testGetLocationsWithNullNodes() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        assertEquals("No nodes means getLocations should return empty array.", 0, qdbInputSplit.getLocations().length);
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getLocations()}.
     */
    @Test
    public void testGetLocations() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        List<String> locationsExpected = Arrays.asList("127.0.0.1");
        try {
            locationsExpected = Arrays.asList(InetAddress.getByName("127.0.0.1").getHostName(), InetAddress.getByName("127.0.0.1").getHostName(), InetAddress.getByName("127.0.0.1").getHostName());
        } catch (UnknownHostException e) { }
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, qdbNodes);
        assertEquals("getLocations should return 3 locations.", 3, qdbInputSplit.getLocations().length);
        assertTrue("Both locations should be equals.", locationsExpected.containsAll(Arrays.asList(qdbInputSplit.getLocations())));
    }

    /**
     * Test method for {@link QuasardbInputSplit#getQdbLocations()}.
     */
    @Test
    public void testGetQdbLocationsWithNullNodes() {
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, null);
        assertEquals("No nodes means getLocations should return empty array.", 0, qdbInputSplit.getQdbLocations().length);
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getQdbLocations()}.
     */
    @Test
    public void testGetQdbLocationsIPV6() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("2607:f0d0:1002:0051:0000:0000:0000:0004", 2836), new QuasardbNode("fe80:0:0:0:200:f8ff:fe21:67cf", 2837) };
        List<String> locationsExpected = Arrays.asList("127.0.0.1");
        try {
            locationsExpected = Arrays.asList(InetAddress.getByName("2607:f0d0:1002:0051:0000:0000:0000:0004").getHostName(), InetAddress.getByName("fe80:0:0:0:200:f8ff:fe21:67cf").getHostName());
        } catch (UnknownHostException e) { }
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, qdbNodes);
        assertEquals("getLocations should return 2 locations.", 2, qdbInputSplit.getLocations().length);
        assertTrue("Both locations should be equals.", locationsExpected.containsAll(Arrays.asList(qdbInputSplit.getLocations())));
    }
    
    /**
     * Test method for {@link QuasardbInputSplit#getQdbLocations()}.
     */
    @Test
    public void testGetQdbLocations() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        List<String> locationsExpected = Arrays.asList("127.0.0.1:2836", "127.0.0.1:2837", "127.0.0.1:2838");
        QuasardbInputSplit qdbInputSplit = new QuasardbInputSplit(null, qdbNodes);
        assertEquals("getLocations should return 3 locations.", 3, qdbInputSplit.getQdbLocations().length);
        List<String> locationsResult = new ArrayList<String>();
        for (QuasardbNode qdbNode : qdbInputSplit.getQdbLocations()) {
            locationsResult.add(qdbNode.toString());
        }
        assertTrue("Both locations should be equals.", locationsExpected.containsAll(locationsResult));
    }
    
    /**
     * Test method for {@link QuasardbPreference#equals(Object)}.
     */
    @Test
    public void testEquals() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        QuasardbInputSplit qdbInputSplit1 = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), qdbNodes);
        QuasardbInputSplit qdbInputSplit2 = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), qdbNodes.clone());
        assertTrue(qdbInputSplit1.equals(qdbInputSplit2));
    }
    
    /**
     * Test method for {@link QuasardbPreference#equals(Object)}.
     */
    @Test
    public void testHashCode() {
        QuasardbNode[] qdbNodes = new QuasardbNode[] { new QuasardbNode("127.0.0.1", 2836), new QuasardbNode("127.0.0.1", 2837), new QuasardbNode("127.0.0.1", 2838) };
        QuasardbInputSplit qdbInputSplit1 = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), qdbNodes);
        QuasardbInputSplit qdbInputSplit2 = new QuasardbInputSplit(Arrays.asList("key1", "key2", "key3"), qdbNodes.clone());
        assertTrue(qdbInputSplit1.hashCode() == qdbInputSplit2.hashCode());
    }
}
