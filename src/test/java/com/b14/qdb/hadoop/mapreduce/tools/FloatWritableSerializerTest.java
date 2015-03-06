package com.b14.qdb.hadoop.mapreduce.tools;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.io.FloatWritable;
import org.junit.Test;

import com.b14.qdb.hadoop.mapreduce.tools.FloatWritableSerializer;
import com.b14.qdb.tools.profiler.Introspector;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A unit test case for {@link FloatWritableSerializer} class.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public class FloatWritableSerializerTest {

    @Test
    public void testFloatWritableSerializer() {
        final Introspector classIntrospector = new Introspector();
        Kryo serializer = new Kryo();
        final FloatWritable value = new FloatWritable(123.12F);
        int bufferSize = 0;
        try {
            bufferSize = (int) classIntrospector.introspect(value).getDeepSize();
        } catch (Exception e) {
            fail("Shouldn't raise an Exception.");
        }
        Output output = new Output(bufferSize);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
        
        // Without serializer => KryoException
        try {
            serializer.writeClassAndObject(output, value);
            fail("Should raise a KryoException.");
        } catch (Exception e) {
            assertTrue(e instanceof KryoException);
        }
        
        // Add custom serializer :
        serializer = new Kryo();
        serializer.setRegistrationRequired(false);
        serializer.setReferences(false);
        serializer.register(FloatWritable.class, new FloatWritableSerializer());
        output = new Output(bufferSize);
        try {
            serializer.writeClassAndObject(output, value);
            buffer.put(output.getBuffer());
        } catch (Exception e) {
            fail("Shouldn't raise an Exception thanks to serializer");
        }
        
        // De-serialize
        buffer.rewind();
        FloatWritable result = (FloatWritable) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        assertTrue(result.get() == value.get());
    }
}
