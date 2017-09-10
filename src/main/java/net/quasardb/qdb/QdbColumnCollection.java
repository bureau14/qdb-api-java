package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;

public class QdbColumnCollection <T> extends ArrayList<QdbColumnValue <T> > {
    QdbColumnDefinition column;

    protected QdbColumnCollection (QdbColumnDefinition column) {
        super();
        this.column = column;
    }

    QdbColumnDefinition getColumn() {
        return this.column;
    }

    public void insert(T value) {
        this.add(new QdbColumnValue <T>(value));
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
