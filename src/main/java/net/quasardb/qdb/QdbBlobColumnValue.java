package net.quasardb.qdb;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import net.quasardb.qdb.jni.*;

public class QdbBlobColumnValue extends QdbColumnValue<ByteBuffer> {

    public QdbBlobColumnValue() {
        super(ByteBuffer.allocateDirect(0));
    }

    public QdbBlobColumnValue(ByteBuffer value) {
        super(value);
    }

    public QdbBlobColumnValue(LocalDateTime timestamp, ByteBuffer value) {
        super(timestamp, value);
    }

    public QdbBlobColumnValue(QdbTimespec timestamp, ByteBuffer value) {
        super(timestamp, value);
    }

    protected static QdbBlobColumnValue fromNative(qdb_ts_blob_point input) {
        return new QdbBlobColumnValue(QdbTimespec.fromNative(input.getTimestamp()),
                                      input.getValue());
    }

    protected static qdb_ts_blob_point toNative(QdbColumnValue<ByteBuffer> point) {
        return new qdb_ts_blob_point(point.getTimestamp().toNative(), point.getValue());
    }

    public String toString() {
        return "QdbBlobColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.hashCode() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbBlobColumnValue)) return false;
        QdbBlobColumnValue rhs = (QdbBlobColumnValue)obj;

        return super.getValue().compareTo(rhs.getValue()) == 0;
    }

    protected void writeValue(java.io.ObjectOutputStream stream, ByteBuffer value)
        throws IOException
    {
        System.out.println("writing value: " + value.toString());

        // :TOOD: if value.hasArray() == true, we can write value.array() directly
        int size = value.capacity();
        byte[] buffer = new byte[size];
        value.get(buffer, 0, size);

        stream.writeInt(value.capacity());
        stream.write(buffer, 0, size);

    }
    protected ByteBuffer readValue(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        int size = stream.readInt();

        System.out.println("reading value. size: " + size);

        byte[] buffer = new byte[size];
        stream.read(buffer, 0, size);

        System.out.println("done reading!");

        ByteBuffer bb = ByteBuffer.allocateDirect(size);
        bb.put(buffer);

        return bb;
    }
}
