package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;

public class QdbTimespec {

    protected LocalDateTime value;

    public QdbTimespec (LocalDateTime value) {
        this.value = value;
    }

    public LocalDateTime getValue() {
        return this.value;
    }

    public qdb_timespec toNative() {
        return new qdb_timespec(this.value.atZone(ZoneId.systemDefault()).toEpochSecond(),
                                this.value.getNano());
    }

    public static QdbTimespec fromNative(qdb_timespec input) {
        return new QdbTimespec(LocalDateTime.ofInstant(Instant.ofEpochSecond(input.getEpochSecond(),
                                                                             input.getNano()),
                                                       ZoneId.systemDefault()));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbTimespec)) return false;
        QdbTimespec rhs = (QdbTimespec)obj;

        System.out.println("lhs = " + this.toString() + ", rhs = " + rhs.toString());
        return true;
    }

    public String toString() {
        return "QdbTimespec (value: " + this.value.toString() + ")";
    }
}
