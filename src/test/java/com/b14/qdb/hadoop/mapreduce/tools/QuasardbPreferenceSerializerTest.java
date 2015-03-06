package com.b14.qdb.hadoop.mapreduce.tools;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

import org.junit.Test;

import com.b14.qdb.hadoop.mahout.QuasardbPreference;
import com.b14.qdb.tools.QuasardbPreferenceSerializer;
import com.b14.qdb.tools.profiler.Introspector;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A unit test case for {@link QuasardbPreferenceSerializer} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class QuasardbPreferenceSerializerTest {
    @Test
    public void testQuasardbPreferenceSerializer() {
        final Introspector classIntrospector = new Introspector();
        Kryo serializer = new Kryo();
        final QuasardbPreference value = new QuasardbPreference(123L, 456L, 0.5F, (new Date()).getTime());
        int bufferSize = 0;
        try {
            bufferSize = (int) classIntrospector.introspect(value).getDeepSize();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
        Output output = new Output(bufferSize);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
        
        try {
            serializer.writeClassAndObject(output, value);
        } catch (Exception e) {
            assertTrue(e instanceof KryoException);
        }
        
        // Add custom serializer :
        serializer = new Kryo();
        serializer.setRegistrationRequired(false);
        serializer.setReferences(false);
        serializer.register(QuasardbPreference.class, new QuasardbPreferenceSerializer());
        output = new Output(bufferSize);
        try {
            serializer.writeClassAndObject(output, value);
            buffer.put(output.getBuffer());
        } catch (Exception e) {
            fail("Shouldn't raise an Exception thanks to serializer");
        }
        
        // De-serialize
        buffer.rewind();
        QuasardbPreference result = (QuasardbPreference) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        assertTrue(result.getUserID() == 123L);
        assertTrue(result.getItemID() == 456L);
    }
    
    @Test
    public void testQuasardbPreferenceSerializerWithoutCreationTime() {
        final Introspector classIntrospector = new Introspector();
        Kryo serializer = new Kryo();
        final QuasardbPreference value = new QuasardbPreference(123L, 456L, 0.5F);
        int bufferSize = 0;
        try {
            bufferSize = (int) classIntrospector.introspect(value).getDeepSize();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
        Output output = new Output(bufferSize);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
        
        try {
            serializer.writeClassAndObject(output, value);
        } catch (Exception e) {
            assertTrue(e instanceof KryoException);
        }
        
        // Add custom serializer :
        serializer = new Kryo();
        serializer.setRegistrationRequired(false);
        serializer.setReferences(false);
        serializer.register(QuasardbPreference.class, new QuasardbPreferenceSerializer());
        output = new Output(bufferSize);
        try {
            serializer.writeClassAndObject(output, value);
            buffer.put(output.getBuffer());
        } catch (Exception e) {
            fail("Shouldn't raise an Exception thanks to serializer");
        }
        
        // De-serialize
        buffer.rewind();
        QuasardbPreference result = (QuasardbPreference) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        assertTrue(result.getUserID() == 123L);
        assertTrue(result.getItemID() == 456L);
    }
}
