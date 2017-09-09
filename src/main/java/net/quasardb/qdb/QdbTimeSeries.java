package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries inside quasardb
 */
public final class QdbTimeSeries {

    QdbSession session;
    String name;

    public enum ColumnType {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int value;
        ColumnType(int type) {
            this.value = type;
        }
    }

    /**
     * Describes a time-series column, required to create the column.
     */
    public static class ColumnDefinition {
        String name;
        ColumnType type;

        ColumnDefinition(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }

        public qdb_ts_column_info toColumnInfo () {
            return new qdb_ts_column_info(this.name, this.type.value);
        }

        public static ColumnDefinition fromNative (qdb_ts_column_info info) {
            return new ColumnDefinition(info.name, ColumnType.values()[info.type]);
        }

        public static ColumnDefinition createBlob(String name) {
            return new ColumnDefinition(name, ColumnType.BLOB);
        }

        public static ColumnDefinition createDouble(String name) {
            return new ColumnDefinition(name, ColumnType.DOUBLE);
        }
    }

    /**
     * A column of a time series
     */
    public abstract static class Column {
        QdbTimeSeries series;
        String name;

        Column(QdbTimeSeries series, String name) {
            this.series = series;
            this.name = name;
        }

        public QdbTimeSeries getTimeSeries() {
            return this.series;
        }

        public String getName() {
            return this.name;
        }
    }

    public static class UnknownColumn extends Column {
        ColumnType type;

        UnknownColumn(QdbTimeSeries series, String  name, ColumnType type) {
            super(series, name);

            this.type = type;
        }

        public ColumnType getType() {
            return this.type;
        }
    }

    QdbTimeSeries(QdbSession session, String name) {
        this.session = session;
        this.name = name;
    }

    public void create(List<ColumnDefinition> columns) {
        qdb_ts_column_info[] columnArray = new qdb_ts_column_info[columns.size()];
        List<qdb_ts_column_info> columnList = new ArrayList<qdb_ts_column_info> ();

        for (ColumnDefinition column : columns) {
            columnList.add(column.toColumnInfo());
        }
        columnList.toArray(columnArray);

        int err = qdb.ts_create(this.session.handle(), this.name, columnArray);
        QdbExceptionFactory.throwIfError(err);
    }

    public List<ColumnDefinition> listColumns () {
        Reference<qdb_ts_column_info[]> nativeColumns = new Reference<qdb_ts_column_info[]>();

        int err = qdb.ts_list_columns(this.session.handle(), this.name, nativeColumns);
        QdbExceptionFactory.throwIfError(err);

        List<ColumnDefinition> columns = new ArrayList<ColumnDefinition> ();
        for (qdb_ts_column_info column : nativeColumns.value) {
            columns.add(ColumnDefinition.fromNative(column));
        }

        return columns;
    }
}
