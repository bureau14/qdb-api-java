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

    QdbTimeSeries(QdbSession session, String name) {
        this.session = session;
        this.name = name;
    }

    public void create(Collection<QdbColumnDefinition> columns) {
        int err = qdb.ts_create(this.session.handle(),
                                this.name,
                                QdbColumnDefinition.toNative(columns));

        QdbExceptionFactory.throwIfError(err);
    }

    public void insertColumns(Collection<QdbColumnDefinition> columns) {
        int err = qdb.ts_insert_columns(this.session.handle(),
                                        this.name,
                                        QdbColumnDefinition.toNative(columns));
        QdbExceptionFactory.throwIfError(err);
    }

    public Iterable<QdbColumnDefinition> listColumns() {
        Reference<qdb_ts_column_info[]> nativeColumns = new Reference<qdb_ts_column_info[]>();

        int err = qdb.ts_list_columns(this.session.handle(), this.name, nativeColumns);
        QdbExceptionFactory.throwIfError(err);

        return QdbColumnDefinition.fromNative(nativeColumns.value);
    }

    public void insertDoubles(QdbDoubleColumnCollection points) {
        int err = qdb.ts_double_insert(this.session.handle(),
                                       this.name,
                                       points.getColumn().getName(),
                                       points.toNative());
        QdbExceptionFactory.throwIfError(err);
    }

    public QdbDoubleColumnCollection getDoubles(String column, QdbTimeRangeCollection ranges) {
        Reference<qdb_ts_double_point[]> points  = new Reference<qdb_ts_double_point[]>();

        System.out.println("querying native ts_double_get_ranges for column: " + column + ", ranges: " + ranges.toString());
        int err = qdb.ts_double_get_ranges(this.session.handle(),
                                           this.name,
                                           column,
                                           ranges.toNative(),
                                           points);
        QdbExceptionFactory.throwIfError(err);

        return QdbDoubleColumnCollection.fromNative(column, points.value);
    }
}
