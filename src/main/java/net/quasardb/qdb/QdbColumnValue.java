package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;
import java.time.LocalTime;

public class QdbColumnValue <T> {

    LocalTime time;
    T value;

    public QdbColumnValue (T value) {
        this.time = LocalTime.now();
        this.value = value;
    }

    public QdbColumnValue (LocalTime time, T value) {
        this.time = time;
        this.value = value;
    }

    public LocalTime getTime() {
        return this.time;
    }

    public T getValue() {
        return this.value;
    }

}
