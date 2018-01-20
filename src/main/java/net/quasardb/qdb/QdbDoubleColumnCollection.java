package net.quasardb.qdb;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.jni.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QdbDoubleColumnCollection extends QdbColumnCollection<QdbDoubleColumnValue> {
    public QdbDoubleColumnCollection (String alias) {
        super(new Column.Double(alias));
    }

    qdb_ts_double_point[] toNative() {
        return this.stream()
            .map(QdbDoubleColumnValue::toNative)
            .toArray(qdb_ts_double_point[]::new);
    }

    static QdbDoubleColumnCollection fromNative(String alias, qdb_ts_double_point[] input) {
        QdbDoubleColumnCollection v = new QdbDoubleColumnCollection(alias);

        List<QdbDoubleColumnValue> values =  Arrays.asList(input).stream()
            .map(QdbDoubleColumnValue::fromNative)
            .collect(Collectors.toCollection(() -> v));

        return v;
    }

}
