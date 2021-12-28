package net.quasardb.qdb;

import java.nio.channels.SeekableByteChannel;

import net.quasardb.qdb.*;
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
        qdb.ts_insert_columns(this.session.handle(),
                              this.name,
                              columns);
    }

    public Column[] listColumns() {
        return Table.getColumns(this.session,
                                this.name);
    }

    public void insertDoubles(QdbDoubleColumnCollection points) {
        qdb.ts_double_insert(this.session.handle(),
                             this.name,
                             points.getColumn().getName(),
                             points.toNative());
    }

    /**
     * Access to timeseries doubles by column.
     */
    public QdbDoubleColumnCollection getDoubles(String column, TimeRange[] ranges) {
        Reference<qdb_ts_double_point[]> points = new Reference<qdb_ts_double_point[]>();

        qdb.ts_double_get_ranges(this.session.handle(),
                                 this.name,
                                 column,
                                 ranges,
                                 points);

        return QdbDoubleColumnCollection.fromNative(column, points.value);
    }

    public QdbDoubleAggregationCollection doubleAggregate(String column, QdbDoubleAggregationCollection input) {
        Reference<qdb_ts_double_aggregation[]> aggregations = new Reference<qdb_ts_double_aggregation[]>();

        qdb.ts_double_aggregate(this.session.handle(),
                                this.name,
                                column,
                                QdbDoubleAggregationCollection.toNative(input),
                                aggregations);

        return QdbDoubleAggregationCollection.fromNative(aggregations.value);
    }

    public void insertBlobs(QdbBlobColumnCollection points) {
        qdb.ts_blob_insert(this.session.handle(),
                           this.name,
                           points.getColumn().getName(),
                           points.toNative());
    }

    /**
     * Access to timeseries blobs by column.
     */
    public QdbBlobColumnCollection getBlobs(String column, TimeRange[] ranges) {
        Reference<qdb_ts_blob_point[]> points = new Reference<qdb_ts_blob_point[]>();

        qdb.ts_blob_get_ranges(this.session.handle(),
                               this.name,
                               column,
                               ranges,
                               points);

        return QdbBlobColumnCollection.fromNative(column, points.value);
    }

    /**
     * Access to timeseries strings by column.
     */
    public QdbStringColumnCollection getStrings(String column, TimeRange[] ranges) {
        Reference<qdb_ts_string_point[]> points = new Reference<qdb_ts_string_point[]>();

        qdb.ts_string_get_ranges(this.session.handle(),
                                 this.name,
                                 column,
                                 ranges,
                                 points);

        return QdbStringColumnCollection.fromNative(column, points.value);
    }

    public QdbBlobAggregationCollection blobAggregate(String column, QdbBlobAggregationCollection input) {
        Reference<qdb_ts_blob_aggregation[]> aggregations = new Reference<qdb_ts_blob_aggregation[]>();

        qdb.ts_blob_aggregate(this.session.handle(),
                              this.name,
                              column,
                              QdbBlobAggregationCollection.toNative(input),
                              aggregations);

        return QdbBlobAggregationCollection.fromNative(aggregations.value);
    }
}
