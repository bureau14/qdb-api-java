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

    public String getName() {
        return this.name;
    }

    public void create(long millisecondsShardSize, Collection<QdbColumnDefinition> columns) {
        int err = qdb.ts_create(this.session.handle(),
                                this.name,
                                millisecondsShardSize,
                                QdbColumnDefinition.toNative(columns));

        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Initializes new timeseries table.
     *
     * @warning Table should be periodically flushed by invoking the .flush() method,
     *          or use autoFlushTable() instead.
     */
    public QdbTimeSeriesTable table() {
        return new QdbTimeSeriesTable(this.session, this.name);
    }

    /**
     * Initializes new timeseries table with auto-flush enabled.
     */
    public QdbTimeSeriesTable autoFlushTable() {
        return new QdbAutoFlushTimeSeriesTable(this.session, this.name);
    }

    /**
     * Initializes new timeseries table with auto-flush enabled.
     *
     * @brioef threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public QdbTimeSeriesTable autoFlushTable(long threshold) {
        return new QdbAutoFlushTimeSeriesTable(this.session, this.name, threshold);
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
        Reference<qdb_ts_double_point[]> points = new Reference<qdb_ts_double_point[]>();

        int err = qdb.ts_double_get_ranges(this.session.handle(),
                                           this.name,
                                           column,
                                           ranges.toNative(),
                                           points);
        QdbExceptionFactory.throwIfError(err);

        return QdbDoubleColumnCollection.fromNative(column, points.value);
    }

    public QdbDoubleAggregationCollection doubleAggregate(String column, QdbDoubleAggregationCollection input) {
        Reference<qdb_ts_double_aggregation[]> aggregations = new Reference<qdb_ts_double_aggregation[]>();

        int err = qdb.ts_double_aggregate(this.session.handle(),
                                          this.name,
                                          column,
                                          QdbDoubleAggregationCollection.toNative(input),
                                          aggregations);
        QdbExceptionFactory.throwIfError(err);

        return QdbDoubleAggregationCollection.fromNative(aggregations.value);
    }

    public void insertBlobs(QdbBlobColumnCollection points) {
        int err = qdb.ts_blob_insert(this.session.handle(),
                                     this.name,
                                     points.getColumn().getName(),
                                     points.toNative());
        QdbExceptionFactory.throwIfError(err);
    }

    public QdbBlobColumnCollection getBlobs(String column, QdbTimeRangeCollection ranges) {
        Reference<qdb_ts_blob_point[]> points = new Reference<qdb_ts_blob_point[]>();

        int err = qdb.ts_blob_get_ranges(this.session.handle(),
                                         this.name,
                                         column,
                                         ranges.toNative(),
                                         points);
        QdbExceptionFactory.throwIfError(err);

        return QdbBlobColumnCollection.fromNative(column, points.value);
    }

    public QdbBlobAggregationCollection blobAggregate(String column, QdbBlobAggregationCollection input) {
        Reference<qdb_ts_blob_aggregation[]> aggregations = new Reference<qdb_ts_blob_aggregation[]>();

        int err = qdb.ts_blob_aggregate(this.session.handle(),
                                        this.name,
                                        column,
                                        QdbBlobAggregationCollection.toNative(input),
                                        aggregations);
        QdbExceptionFactory.throwIfError(err);

        return QdbBlobAggregationCollection.fromNative(aggregations.value);
    }
}
