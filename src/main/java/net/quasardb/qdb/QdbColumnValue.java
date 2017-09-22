package net.quasardb.qdb;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.time.LocalDateTime;
import net.quasardb.qdb.jni.*;

public abstract class QdbColumnValue <T> implements Serializable {

    QdbTimespec timestamp;
    protected T value;

    public QdbColumnValue (T value) {
        this(LocalDateTime.now(), value);
    }

    public QdbColumnValue (Timestamp timestamp, T value) {
        this(new QdbTimespec(timestamp), value);
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

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(this.timestamp);

        // :TODO: we should provide a default implementation that uses
        // the regular serialization interface in case it is possible.
        this.writeValue(stream, value);
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        this.timestamp = (QdbTimespec)(stream.readObject());

        // :TODO: we should provide a default implementation that uses
        // the regular serialization interface in case it is possible.
        this.value = (T)(this.readValue(stream));
    }

    protected abstract void writeValue(java.io.ObjectOutputStream stream, T value) throws IOException;
    protected abstract T readValue(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException;

    public String toString() {
        return "QdbColumnValue (timestamp: " + this.timestamp.toString() + ", value: " + this.value.toString() + ")";
    }
}
