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
     * Initializes new timeseries table writer.
     *
     * Table writer should be periodically flushed by invoking the .flush()
     * method, or create a new table writer using autoFlushTableWriter() instead.
     */
    public QdbTimeSeriesWriter tableWriter() {
        return QdbTimeSeriesTable.writer(this.session, this.name);
    }

    /**
     * Initializes new timeseries table writer with auto-flush enabled.
     */
    public QdbAutoFlushTimeSeriesWriter autoFlushTableWriter() {
        return QdbTimeSeriesTable.autoFlushWriter(this.session, this.name);
    }

    /**
     * Initializes new timeseries table writer with auto-flush enabled.
     *
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public QdbAutoFlushTimeSeriesWriter autoFlushTableWriter(long threshold) {
        return QdbTimeSeriesTable.autoFlushWriter(this.session, this.name, threshold);
    }

    /**
     * Initializes new timeseries table reader.
     */
    public QdbTimeSeriesReader tableReader(QdbTimeRange[] ranges) {
        return tableReader(Arrays.stream(ranges).toArray(QdbFilteredRange[]::new));
    }

    /**
     * Initializes new timeseries table reader.
     */
    public QdbTimeSeriesReader tableReader(QdbFilteredRange[] ranges) {
        return QdbTimeSeriesTable.reader(this.session, this.name, ranges);
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

    /**
     * Access to timeseries doubles by column. Helper function that automatically converts
     * QdbTimeRange objects to QdbFilteredRange objects without filter;
     */
    public QdbDoubleColumnCollection getDoubles(String column, QdbTimeRange[] ranges) {
        return getDoubles(column,
                          Arrays.stream(ranges).toArray(QdbFilteredRange[]::new));
    }

    /**
     * Access to timeseries doubles by column.
     */
    public QdbDoubleColumnCollection getDoubles(String column, QdbFilteredRange[] ranges) {
        Reference<qdb_ts_double_point[]> points = new Reference<qdb_ts_double_point[]>();

        int err = qdb.ts_double_get_ranges(this.session.handle(),
                                           this.name,
                                           column,
                                           ranges,
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


    /**
     * Access to timeseries blobs by column. Helper function that automatically converts
     * QdbTimeRange objects to QdbFilteredRange objects without filter.
     */
    public QdbBlobColumnCollection getBlobs(String column, QdbTimeRange[] ranges) {
        return getBlobs(column,
                        Arrays.stream(ranges).toArray(QdbFilteredRange[]::new));
    }

    /**
     * Access to timeseries blobs by column.
     */
    public QdbBlobColumnCollection getBlobs(String column, QdbFilteredRange[] ranges) {
        Reference<qdb_ts_blob_point[]> points = new Reference<qdb_ts_blob_point[]>();

        int err = qdb.ts_blob_get_ranges(this.session.handle(),
                                         this.name,
                                         column,
                                         ranges,
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
