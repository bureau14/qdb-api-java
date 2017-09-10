package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

public class QdbTimeRange {

    protected QdbTimespec begin;
    protected QdbTimespec end;

    public QdbTimeRange (QdbTimespec begin, QdbTimespec end) {
        this.begin = begin;
        this.end = end;
    }

    public QdbTimespec getBegin() {
        return this.begin;
    }

    public QdbTimespec getEnd() {
        return this.end;
    }
    public qdb_ts_range toNative() {
        return new qdb_ts_range(begin.toNative(), end.toNative());
    }

    public String toString() {
        return "QdbTimeRange (begin: " + this.begin.toString() + ", end: " + this.end.toString() + ")";
    }
}
