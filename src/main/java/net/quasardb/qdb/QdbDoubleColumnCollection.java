package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;


public class QdbDoubleColumnCollection extends QdbColumnCollection<Double> {
    QdbColumnDefinition column;

    public QdbDoubleColumnCollection (String alias) {
        super(new QdbColumnDefinition.Double(alias));
    }

    qdb_ts_double_point[] toNative() {
        return this.stream()
            .map(QdbDoubleColumnCollection::pointToNative)
            .toArray(qdb_ts_double_point[]::new);
    }

    private static qdb_ts_double_point pointToNative(QdbColumnValue<Double> point) {
        return new qdb_ts_double_point(point.getTimestamp().toNative(), point.getValue());
    }
}
