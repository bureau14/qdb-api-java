package net.quasardb.qdb;

import java.io.*;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;

import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.jni.*;

public class QdbBlobColumnValue extends QdbColumnValue<ByteBuffer> {

    public QdbBlobColumnValue() {
        super(ByteBuffer.allocateDirect(0));
    }

    public QdbBlobColumnValue(ByteBuffer value) {
        super(value);
    }

    public QdbBlobColumnValue(Timestamp timestamp, ByteBuffer value) {
        super(timestamp, value);
    }

    public QdbBlobColumnValue(LocalDateTime timestamp, ByteBuffer value) {
        super(timestamp, value);
    }

    public QdbBlobColumnValue(Timespec timestamp, ByteBuffer value) {
        super(timestamp, value);
    }

    protected static QdbBlobColumnValue fromNative(qdb_ts_blob_point input) {
        return new QdbBlobColumnValue(input.getTimestamp(),
                                      input.getValue());
    }

    protected static qdb_ts_blob_point toNative(QdbColumnValue<ByteBuffer> point) {
        return new qdb_ts_blob_point(point.getTimestamp(), point.getValue());
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
        // :TOOD: if value.hasArray() == true, we can write value.array() directly
        int size = value.capacity();

        byte[] buffer = new byte[size];
        value.get(buffer, 0, size);

        stream.writeInt(value.capacity());
        stream.write(buffer, 0, size);

        value.rewind();

    }
    protected ByteBuffer readValue(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        int size = stream.readInt();

        byte[] buffer = new byte[size];
        stream.read(buffer, 0, size);

        ByteBuffer bb = ByteBuffer.allocateDirect(size);
        bb.put(buffer, 0, size);
        bb.rewind();

        return bb.duplicate();
    }
}
