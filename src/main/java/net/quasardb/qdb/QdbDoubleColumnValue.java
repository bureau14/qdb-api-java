package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.time.LocalDateTime;

public class QdbDoubleColumnValue extends QdbColumnValue<Double> {

    public QdbDoubleColumnValue(Double value) {
        super(value);
    }

    public QdbDoubleColumnValue(LocalDateTime timestamp, Double value) {
        super(timestamp, value);
    }

    public QdbDoubleColumnValue(QdbTimespec timestamp, Double value) {
        super(timestamp, value);
    }


    protected static QdbDoubleColumnValue fromNative(qdb_ts_double_point input) {
        return new QdbDoubleColumnValue(QdbTimespec.fromNative(input.getTimestamp()),
                                        input.getValue());
    }

    public String toString() {
        return "QdbDoubleColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.toString() + ")";
    }
}
