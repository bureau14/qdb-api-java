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

    public void create(Collection<QdbColumnDefinition> columns) {
        int err = qdb.ts_create(this.session.handle(),
                                this.name,
                                QdbColumnDefinition.toNative(columns));

        QdbExceptionFactory.throwIfError(err);
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
}
