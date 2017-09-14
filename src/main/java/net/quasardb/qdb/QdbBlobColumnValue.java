package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import net.quasardb.qdb.jni.*;

public class QdbBlobColumnValue extends QdbColumnValue<ByteBuffer> {

    public QdbBlobColumnValue(ByteBuffer value) {
        this(LocalDateTime.now(),
             value);
    }

    public QdbBlobColumnValue(LocalDateTime timestamp, ByteBuffer value) {
        this(new QdbTimespec(timestamp), value);
    }

    public QdbBlobColumnValue(QdbTimespec timestamp, ByteBuffer value) {
        super.timestamp = timestamp;
        super.value = value;
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
}
