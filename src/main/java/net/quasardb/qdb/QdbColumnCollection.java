package net.quasardb.qdb;

import java.util.ArrayList;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.jni.*;

public class QdbColumnCollection <T extends QdbColumnValue> extends ArrayList<T> {
    Column column;

    protected QdbColumnCollection (Column column) {
        super();
        this.column = column;
    }

    Column getColumn() {
        return this.column;
    }

    /**
     * Returns the interval that contains all values within this column.
     */
    public TimeRange range() {
        TimeRange cur = null;

        for (T val : this) {
            if (cur == null ){
                cur = new TimeRange(val.getTimestamp(), val.getTimestamp());
            } else {
                if (val.getTimestamp().asLocalDateTime().isBefore(cur.getBegin().asLocalDateTime())) {
                    cur = cur.withBegin(val.getTimestamp());
                }

                if (val.getTimestamp().asLocalDateTime().isAfter(cur.getEnd().asLocalDateTime())) {
                    cur = cur.withEnd(val.getTimestamp());
                }
            }
        }

        return cur;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbDoubleColumnCollection)) return false;
        QdbColumnCollection rhs = (QdbColumnCollection)obj;

        return super.equals(rhs) && this.column.equals(rhs.column);
    }

    public String toString() {
        return "QdbColumnCollection (column: '" + this.column.toString() + ", values: " + super.toString() + ")";
    }
}
