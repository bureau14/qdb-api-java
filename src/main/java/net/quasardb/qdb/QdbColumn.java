package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a column inside qdb
 */
public final class QdbColumn {
    String name;

    public enum Type {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int value;
        Type(int type) {
            this.value = type;
        }
    }

    public static class Definition {
        String name;
        Type type;

        public static class Blob extends Definition {
            public Blob(String name) {
                super(name, Type.BLOB);
            }
        }

        public static class Double extends Definition {
            public Double(String name) {
                super(name, Type.DOUBLE);
            }
        }

        Definition(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public static qdb_ts_column_info toNative (Definition d) {
            return new qdb_ts_column_info(d.name, d.type.value);
        }

        public static qdb_ts_column_info[] toNative (Collection<Definition> columns) {
            qdb_ts_column_info[] columnArray = new qdb_ts_column_info[columns.size()];
            List<qdb_ts_column_info> columnList = new ArrayList<qdb_ts_column_info> ();

            for (QdbColumn.Definition column : columns) {
                columnList.add(toNative(column));
            }
            columnList.toArray(columnArray);

            return columnArray;
        }

        public static Definition fromNative (qdb_ts_column_info info) {
            switch (info.type) {
            case qdb_ts_column_type.double_:
                return new Double(info.name);
            case qdb_ts_column_type.blob:
                return new Blob(info.name);
            default:
                throw new IllegalArgumentException("invalid column type: " + info.type);
            }
        }

        public static Iterable<Definition> fromNative (qdb_ts_column_info[] nativeColumns) {
            Collection<QdbColumn.Definition> columns = new ArrayList<QdbColumn.Definition> ();
            for (qdb_ts_column_info column : nativeColumns) {
                columns.add(fromNative(column));
            }
            return columns;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Definition)) return false;
            Definition rhs = (Definition)obj;

            return this.name.compareTo(rhs.name) == 0 && this.type == rhs.type;
        }

        public String toString() {
            return "QdbColumn.Definition (name: '" + this.name + "', type: " + this.type + ")";
        }
    }

    QdbColumn(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
