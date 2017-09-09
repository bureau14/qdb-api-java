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

        public qdb_ts_column_info toColumnInfo () {
            return new qdb_ts_column_info(this.name, this.type.value);
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
