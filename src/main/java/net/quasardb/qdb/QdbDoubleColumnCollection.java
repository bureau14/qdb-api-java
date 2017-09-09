package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;

public class QdbDoubleColumnCollection extends ArrayList<Double> {
    QdbColumnDefinition column;

    public QdbDoubleColumnCollection (String alias) {
        super();
        this.column = new QdbColumnDefinition.Double(alias);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbDoubleColumnCollection)) return false;
        QdbDoubleColumnCollection rhs = (QdbDoubleColumnCollection)obj;

        return super.equals(obj) && this.column == rhs.column;
    }

    public String toString() {
        return "QdbDoubleColumnCollection (column: '" + this.column.toString() + ")";
    }
}
