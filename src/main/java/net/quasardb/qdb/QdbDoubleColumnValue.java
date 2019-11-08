package net.quasardb.qdb;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.jni.*;

public class QdbDoubleColumnValue extends QdbColumnValue<Double> {

    public QdbDoubleColumnValue() {
        super(-1.0);
    }

    public QdbDoubleColumnValue(Double value) {
        super(value);
    }

    public QdbDoubleColumnValue(Timestamp timestamp, Double value) {
        super(timestamp, value);
    }

    public QdbDoubleColumnValue(LocalDateTime timestamp, Double value) {
        super(timestamp, value);
    }

    public QdbDoubleColumnValue(Timespec timestamp, Double value) {
        super(timestamp, value);
    }

    protected static QdbDoubleColumnValue fromNative(qdb_ts_double_point input) {
        return
            input != null
            ? new QdbDoubleColumnValue(input.getTimestamp(),
                                       input.getValue())
            : null;
    }

    protected static qdb_ts_double_point toNative(QdbColumnValue<Double> point) {
        return new qdb_ts_double_point(point.getTimestamp(), point.getValue());
    }

    public String toString() {
        return "QdbDoubleColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.toString() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbDoubleColumnValue)) return false;
        QdbDoubleColumnValue rhs = (QdbDoubleColumnValue)obj;

        return true;
    }

    protected void writeValue(java.io.ObjectOutputStream stream, Double value) throws IOException {
        stream.writeObject(value);
    }
    protected Double readValue(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
        return (Double)(stream.readObject());
    }

}
