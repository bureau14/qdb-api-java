package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.time.LocalDateTime;

public class QdbTimespec {

    LocalDateTime value;

    public QdbTimespec (LocalDateTime value) {
        this.value = value;
    }

    public qdb_timespec toNative() {
        return new qdb_timespec(0, 0);
    }

    public String toString() {
        return "QdbTimespec (value: " + this.value.toString() + ")";
    }
}
