package com.b14.qdb.tools;

import com.b14.qdb.hadoop.mahout.QuasardbPreference;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo specific serializer for {@link QuasardbPreference}
 *
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @see QuasardbPreference
 * @since 1.3.0
 */
public class QuasardbPreferenceSerializer extends Serializer<QuasardbPreference> {
    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public QuasardbPreference read(Kryo kryo, Input input, Class<QuasardbPreference> type) {
        return new QuasardbPreference(input.readLong(), input.readLong(), input.readFloat(), input.readLong());
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public void write(Kryo kryo, Output output, QuasardbPreference object) {
        output.writeLong(object.getUserID());
        output.writeLong(object.getItemID());
        output.writeFloat(object.getValue());
        output.writeLong(object.getCreateAt());
    }
}
