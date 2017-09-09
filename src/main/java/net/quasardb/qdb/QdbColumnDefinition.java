package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;


public class QdbColumnDefinition {
    protected String name;
    protected Type type;

    public enum Type {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int value;
        Type(int type) {
            this.value = type;
        }
    }

    public static class Blob extends QdbColumnDefinition {
        public Blob(String name) {
            super(name, Type.BLOB);
        }
    }

    public static class Double extends QdbColumnDefinition {
        public Double(String name) {
            super(name, Type.DOUBLE);
        }
    }

    QdbColumnDefinition(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public static qdb_ts_column_info toNative (QdbColumnDefinition d) {
        return new qdb_ts_column_info(d.name, d.type.value);
    }

    public static qdb_ts_column_info[] toNative (Collection<QdbColumnDefinition> columns) {
        qdb_ts_column_info[] columnArray = new qdb_ts_column_info[columns.size()];
        List<qdb_ts_column_info> columnList = new ArrayList<qdb_ts_column_info> ();

        for (QdbColumnDefinition column : columns) {
            columnList.add(toNative(column));
        }
        columnList.toArray(columnArray);

        return columnArray;
    }

    public static QdbColumnDefinition fromNative (qdb_ts_column_info info) {
        switch (info.type) {
        case qdb_ts_column_type.double_:
            return new Double(info.name);
        case qdb_ts_column_type.blob:
            return new Blob(info.name);
        default:
            throw new IllegalArgumentException("invalid column type: " + info.type);
        }
    }

    public static Iterable<QdbColumnDefinition> fromNative (qdb_ts_column_info[] nativeColumns) {
        Collection<QdbColumnDefinition> columns = new ArrayList<QdbColumnDefinition> ();
        for (qdb_ts_column_info column : nativeColumns) {
            columns.add(fromNative(column));
        }
        return columns;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbColumnDefinition)) return false;
        QdbColumnDefinition rhs = (QdbColumnDefinition)obj;

        return this.name.compareTo(rhs.name) == 0 && this.type == rhs.type;
    }

    public String toString() {
        return "QdbColumnDefinition (name: '" + this.name + "', type: " + this.type + ")";
    }
}
