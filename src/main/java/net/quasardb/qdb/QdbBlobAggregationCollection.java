package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QdbBlobAggregationCollection extends ArrayList<QdbBlobAggregation> {

    public static qdb_ts_blob_aggregation[] toNative(QdbBlobAggregationCollection input) {
        return input.stream()
            .map(QdbBlobAggregation::toNative)
            .toArray(qdb_ts_blob_aggregation[]::new);
    }


    public static QdbBlobAggregationCollection fromNative(qdb_ts_blob_aggregation[] input) {
        QdbBlobAggregationCollection v = new QdbBlobAggregationCollection();

        Arrays.asList(input).stream()
            .map(QdbBlobAggregation::fromNative)
            .collect(Collectors.toCollection(() -> v));

        return v;
    }
}
