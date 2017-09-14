package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;
import java.time.LocalDateTime;

public class QdbColumnValue <T> {

    QdbTimespec timestamp;
    protected T value;

    public QdbColumnValue (T value) {
        this(LocalDateTime.now(), value);
    }

    public QdbColumnValue (LocalDateTime timestamp, T value) {
        this(new QdbTimespec(timestamp), value);
    }

    public QdbColumnValue (QdbTimespec timestamp, T value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public QdbTimespec getTimestamp() {
        return this.timestamp;
    }

    public T getValue() {
        return this.value;
    }

    public String toString() {
        return "QdbColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.toString() + ")";
    }
}
