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

        Definition(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public qdb_ts_column_info toColumnInfo () {
            return new qdb_ts_column_info(this.name, this.type.value);
        }

        public static Definition fromNative (qdb_ts_column_info info) {
            return new Definition(info.name, Type.values()[info.type]);
        }

        public static Definition createBlob(String name) {
            return new Definition(name, Type.BLOB);
        }

        public static Definition createDouble(String name) {
            return new Definition(name, Type.DOUBLE);
        }
    }

    QdbColumn(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
