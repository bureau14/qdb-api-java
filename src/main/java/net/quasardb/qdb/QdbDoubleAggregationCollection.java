package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QdbDoubleAggregationCollection extends ArrayList<QdbDoubleAggregation> {

    public static qdb_ts_double_aggregation[] toNative(QdbDoubleAggregationCollection input) {
        return input.stream()
            .map(QdbDoubleAggregation::toNative)
            .toArray(qdb_ts_double_aggregation[]::new);
    }


    public static QdbDoubleAggregationCollection fromNative(qdb_ts_double_aggregation[] input) {
        QdbDoubleAggregationCollection v = new QdbDoubleAggregationCollection();


        System.out.println("converting double aggregation collection, length: " + Arrays.asList(input).size());

        Arrays.asList(input).stream()
            .map(QdbDoubleAggregation::fromNative)
            .collect(Collectors.toCollection(() -> v));

        return v;
    }
}
