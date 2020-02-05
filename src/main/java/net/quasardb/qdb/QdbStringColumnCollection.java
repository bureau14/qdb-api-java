package net.quasardb.qdb;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.jni.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QdbStringColumnCollection extends QdbColumnCollection<QdbStringColumnValue> {
    public QdbStringColumnCollection (String alias) {
        super(new Column.String_(alias));
    }

    qdb_ts_string_point[] toNative() {
        return this.stream()
            .map(QdbStringColumnValue::toNative)
            .toArray(qdb_ts_string_point[]::new);
    }

    static QdbStringColumnCollection fromNative(String alias, qdb_ts_string_point[] input) {
        QdbStringColumnCollection v = new QdbStringColumnCollection(alias);

        List<QdbStringColumnValue> values =  Arrays.asList(input).stream()
            .map(QdbStringColumnValue::fromNative)
            .collect(Collectors.toCollection(() -> v));

        return v;
    }
}
