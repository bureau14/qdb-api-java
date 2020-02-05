package net.quasardb.qdb;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.jni.*;

public class QdbStringColumnValue extends QdbColumnValue<String> {

    public QdbStringColumnValue() {
        super("");
    }

    public QdbStringColumnValue(String value) {
        super(value);
    }

    public QdbStringColumnValue(Timestamp timestamp, String value) {
        super(timestamp, value);
    }

    public QdbStringColumnValue(LocalDateTime timestamp, String value) {
        super(timestamp, value);
    }

    public QdbStringColumnValue(Timespec timestamp, String value) {
        super(timestamp, value);
    }

    protected static QdbStringColumnValue fromNative(qdb_ts_string_point input) {
        return
            input != null
            ? new QdbStringColumnValue(input.getTimestamp(),
                                     input.getValue())
            : null;
    }

    protected static qdb_ts_string_point toNative(QdbColumnValue<String> point) {
        return new qdb_ts_string_point(point.getTimestamp(), point.getValue());
    }

    public String toString() {
        return "QdbStringColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.hashCode() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbStringColumnValue)) return false;
        QdbStringColumnValue rhs = (QdbStringColumnValue)obj;

        return super.getValue().compareTo(rhs.getValue()) == 0;
    }

    protected void writeValue(java.io.ObjectOutputStream stream, String value)
        throws IOException
    {
        stream.writeObject(value);

    }
    protected String readValue(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        return (String)stream.readObject();
    }
}
