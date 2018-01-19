package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.jni.*;

public class QdbColumnDefinition {
    protected String name;
    protected Value.Type type;

    public static class Blob extends QdbColumnDefinition {
        public Blob(String name) {
            super(name, Value.Type.BLOB);
        }
    }

    public static class Double extends QdbColumnDefinition {
        public Double(String name) {
            super(name, Value.Type.DOUBLE);
        }
    }

    public static class Int64 extends QdbColumnDefinition {
        public Int64(String name) {
            super(name, Value.Type.INT64);
        }
    }


    public static class Timestamp extends QdbColumnDefinition {
        public Timestamp(String name) {
            super(name, Value.Type.TIMESTAMP);
        }
    }

    QdbColumnDefinition(String name, Value.Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Value.Type getType() {
        return this.type;
    }

    public static qdb_ts_column_info toNative (QdbColumnDefinition d) {
        return new qdb_ts_column_info(d.getName(), d.getType().asInt());
    }

    public static qdb_ts_column_info[] toNative (Collection<QdbColumnDefinition> columns) {
        return columns.stream()
            .map(QdbColumnDefinition::toNative)
            .toArray(qdb_ts_column_info[]::new);
    }

    public static QdbColumnDefinition fromNative (qdb_ts_column_info info) {
        return new QdbColumnDefinition(info.name,
                                       Value.Type.fromInt(info.type));
    }

    public static Iterable<QdbColumnDefinition> fromNative (qdb_ts_column_info[] nativeColumns) {
        return Arrays.asList(nativeColumns).stream()
            .map(QdbColumnDefinition::fromNative)
            .collect(Collectors.toList());
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
