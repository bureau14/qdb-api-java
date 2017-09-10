package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;


public class QdbDoubleColumnCollection extends QdbColumnCollection<QdbDoubleColumnValue> {
    QdbColumnDefinition column;

    public QdbDoubleColumnCollection (String alias) {
        super(new QdbColumnDefinition.Double(alias));
    }

    qdb_ts_double_point[] toNative() {
        return this.stream()
            .map(QdbDoubleColumnCollection::pointToNative)
            .toArray(qdb_ts_double_point[]::new);
    }

    static QdbDoubleColumnCollection fromNative(String alias, qdb_ts_double_point[] input) {
        QdbDoubleColumnCollection v = new QdbDoubleColumnCollection(alias);

        List<QdbDoubleColumnValue> values =  Arrays.asList(input).stream()
            .map(QdbDoubleColumnValue::fromNative)
            .collect(Collectors.toCollection(() -> v));

        System.out.println("collection = " + v.toString());

        return v;
    }

    private static qdb_ts_double_point pointToNative(QdbColumnValue<Double> point) {
        return new qdb_ts_double_point(point.getTimestamp().toNative(), point.getValue());
    }
}
