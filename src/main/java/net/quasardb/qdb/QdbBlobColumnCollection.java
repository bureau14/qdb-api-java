package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QdbBlobColumnCollection extends QdbColumnCollection<QdbBlobColumnValue> {
    public QdbBlobColumnCollection (String alias) {
        super(new QdbColumnDefinition.Blob(alias));
    }

    qdb_ts_blob_point[] toNative() {
        return this.stream()
            .map(QdbBlobColumnValue::toNative)
            .toArray(qdb_ts_blob_point[]::new);
    }

    static QdbBlobColumnCollection fromNative(String alias, qdb_ts_blob_point[] input) {
        QdbBlobColumnCollection v = new QdbBlobColumnCollection(alias);

        List<QdbBlobColumnValue> values =  Arrays.asList(input).stream()
            .map(QdbBlobColumnValue::fromNative)
            .collect(Collectors.toCollection(() -> v));

        return v;
    }
}
