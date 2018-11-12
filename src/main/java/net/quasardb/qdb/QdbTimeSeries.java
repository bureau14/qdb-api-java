package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.jni.*;

import java.util.*;

/**
 * Legacy interface to QdbTimeSeries, which provides legacy compatibility
 * functions.
 */
public final class QdbTimeSeries {

    Session session;
    String name;

    QdbTimeSeries(Session session, String name) {
        this.session = session;
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * Create new timeseries table with a collection of columns.
     */
    public void create(long millisecondsShardSize, Column[] columns) {
        Table.create(this.session, this.name, columns, millisecondsShardSize);
    }

    /**
     * Initializes new timeseries table writer.
     *
     * Table writer should be periodically flushed by invoking the .flush()
     * method, or create a new table writer using autoFlushTableWriter() instead.
     */
    public Writer tableWriter() {
        return Table.writer(this.session, this.name);
    }

    /**
     * Initializes new timeseries table writer that makes use of high-speed buffered
     * writes.
     *
     * Table writer should be periodically flushed by invoking the .flush()
     * method, or create a new table writer using autoFlushTableWriter() instead.
     */
    public Writer asyncTableWriter() {
        return Table.asyncWriter(this.session, this.name);
    }

    /**
     * Initializes new timeseries table writer with auto-flush enabled.
     */
    public AutoFlushWriter autoFlushTableWriter() {
        return Table.autoFlushWriter(this.session, this.name);
    }

    /**
     * Initializes new timeseries table writer with auto-flush enabled.
     *
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public AutoFlushWriter autoFlushTableWriter(long threshold) {
        return Table.autoFlushWriter(this.session, this.name, threshold);
    }

    /**
     * Initializes new timeseries table reader.
     */
    public Reader tableReader(TimeRange[] ranges) {
        return Table.reader(this.session, this.name, ranges);
    }

    public void insertColumns(Column[] columns) {
        int err = qdb.ts_insert_columns(this.session.handle(),
                                        this.name,
                                        columns);
        ExceptionFactory.throwIfError(err);
    }

    public Column[] listColumns() {
        Reference<Column[]> nativeColumns = new Reference<Column[]>();

        int err = qdb.ts_list_columns(this.session.handle(), this.name, nativeColumns);
        ExceptionFactory.throwIfError(err);

        return nativeColumns.value;
    }

    public void insertDoubles(QdbDoubleColumnCollection points) {
        int err = qdb.ts_double_insert(this.session.handle(),
                                       this.name,
                                       points.getColumn().getName(),
                                       points.toNative());
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Access to timeseries doubles by column.
     */
    public QdbDoubleColumnCollection getDoubles(String column, TimeRange[] ranges) {
        Reference<qdb_ts_double_point[]> points = new Reference<qdb_ts_double_point[]>();

        int err = qdb.ts_double_get_ranges(this.session.handle(),
                                           this.name,
                                           column,
                                           ranges,
                                           points);
        ExceptionFactory.throwIfError(err);

        return QdbDoubleColumnCollection.fromNative(column, points.value);
    }

    public QdbDoubleAggregationCollection doubleAggregate(String column, QdbDoubleAggregationCollection input) {
        Reference<qdb_ts_double_aggregation[]> aggregations = new Reference<qdb_ts_double_aggregation[]>();

        int err = qdb.ts_double_aggregate(this.session.handle(),
                                          this.name,
                                          column,
                                          QdbDoubleAggregationCollection.toNative(input),
                                          aggregations);
        ExceptionFactory.throwIfError(err);

        return QdbDoubleAggregationCollection.fromNative(aggregations.value);
    }

    public void insertBlobs(QdbBlobColumnCollection points) {
        int err = qdb.ts_blob_insert(this.session.handle(),
                                     this.name,
                                     points.getColumn().getName(),
                                     points.toNative());
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Access to timeseries blobs by column.
     */
    public QdbBlobColumnCollection getBlobs(String column, TimeRange[] ranges) {
        Reference<qdb_ts_blob_point[]> points = new Reference<qdb_ts_blob_point[]>();

        int err = qdb.ts_blob_get_ranges(this.session.handle(),
                                         this.name,
                                         column,
                                         ranges,
                                         points);
        ExceptionFactory.throwIfError(err);

        return QdbBlobColumnCollection.fromNative(column, points.value);
    }

    public QdbBlobAggregationCollection blobAggregate(String column, QdbBlobAggregationCollection input) {
        Reference<qdb_ts_blob_aggregation[]> aggregations = new Reference<qdb_ts_blob_aggregation[]>();

        int err = qdb.ts_blob_aggregate(this.session.handle(),
                                        this.name,
                                        column,
                                        QdbBlobAggregationCollection.toNative(input),
                                        aggregations);
        ExceptionFactory.throwIfError(err);

        return QdbBlobAggregationCollection.fromNative(aggregations.value);
    }
}
