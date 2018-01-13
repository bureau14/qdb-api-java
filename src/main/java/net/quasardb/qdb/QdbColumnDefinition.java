package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;
import java.util.stream.Collectors;

public class QdbColumnDefinition {
    protected String name;
    protected QdbTimeSeriesValue.Type type;

    public static class Blob extends QdbColumnDefinition {
        public Blob(String name) {
            super(name, QdbTimeSeriesValue.Type.BLOB);
        }
    }

    public static class Double extends QdbColumnDefinition {
        public Double(String name) {
            super(name, QdbTimeSeriesValue.Type.DOUBLE);
        }
    }

    public static class Int64 extends QdbColumnDefinition {
        public Int64(String name) {
            super(name, QdbTimeSeriesValue.Type.INT64);
        }
    }

    public static class Timestamp extends QdbColumnDefinition {
        public Timestamp(String name) {
            super(name, QdbTimeSeriesValue.Type.TIMESTAMP);
        }
    }

    QdbColumnDefinition(String name, QdbTimeSeriesValue.Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public QdbTimeSeriesValue.Type getType() {
        return this.type;
    }

    public static qdb_ts_column_info toNative (QdbColumnDefinition d) {
        return new qdb_ts_column_info(d.name, d.type.value);
    }

    public static qdb_ts_column_info[] toNative (Collection<QdbColumnDefinition> columns) {
        return columns.stream()
            .map(QdbColumnDefinition::toNative)
            .toArray(qdb_ts_column_info[]::new);
    }

    public static QdbColumnDefinition fromNative (qdb_ts_column_info info) {
        return new QdbColumnDefinition(info.name,
                                       QdbTimeSeriesValue.Type.fromInt(info.type));
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
