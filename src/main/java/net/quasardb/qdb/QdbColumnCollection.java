package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;

public class QdbColumnCollection <T extends QdbColumnValue> extends ArrayList<T> {
    QdbColumnDefinition column;

    protected QdbColumnCollection (QdbColumnDefinition column) {
        super();
        this.column = column;
    }

    QdbColumnDefinition getColumn() {
        return this.column;
    }

    /**
     * Returns the interval that contains all values within this column.
     */
    public QdbTimeRange range() {
        QdbTimeRange cur = null;

        for (T val : this) {
            if (cur == null ){
                cur = new QdbTimeRange(val.getTimestamp(), val.getTimestamp());
            } else {
                if (val.getTimestamp().value.isBefore(cur.begin.value)) {
                    cur.begin = val.getTimestamp();
                }

                if (val.getTimestamp().value.isAfter(cur.end.value)) {
                    cur.end = val.getTimestamp();
                }
            }
        }

        return cur;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbDoubleColumnCollection)) return false;
        QdbColumnCollection rhs = (QdbColumnCollection)obj;

        return super.equals(obj) && this.column == rhs.column;
    }

    public String toString() {
        return "QdbColumnCollection (column: '" + this.column.toString() + ", values: " + super.toString() + ")";
    }
}
