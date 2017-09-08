package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.ArrayList;
import java.util.List;

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

        protected final int type;
        ColumnType(int type) {
            this.type = type;
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
            return new qdb_ts_column_info(this.name, this.type.ordinal());
        }
    }

    /**
     * Describes a time-series column that contains blobs
     */
    public static class BlobColumnDefinition extends ColumnDefinition {
        public BlobColumnDefinition(String name) {
            super(name, ColumnType.BLOB);
        }
    }

    /**
     * Describes a time-series column that contains double precision floating point values
     */
    public static class DoubleColumnDefinition extends ColumnDefinition {
        public DoubleColumnDefinition(String name) {
            super(name, ColumnType.DOUBLE);
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

        qdb.ts_create(this.session.handle(), this.name, columnArray);
    }

}
