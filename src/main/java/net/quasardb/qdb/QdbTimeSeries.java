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

    public void create(Collection<QdbColumn.Definition> columns) {
        qdb_ts_column_info[] columnArray = new qdb_ts_column_info[columns.size()];
        List<qdb_ts_column_info> columnList = new ArrayList<qdb_ts_column_info> ();

        for (QdbColumn.Definition column : columns) {
            columnList.add(column.toColumnInfo());
        }
        columnList.toArray(columnArray);

        int err = qdb.ts_create(this.session.handle(), this.name, columnArray);
        QdbExceptionFactory.throwIfError(err);
    }

    public Iterable<QdbColumn.Definition> listColumns () {
        Reference<qdb_ts_column_info[]> nativeColumns = new Reference<qdb_ts_column_info[]>();

        int err = qdb.ts_list_columns(this.session.handle(), this.name, nativeColumns);
        QdbExceptionFactory.throwIfError(err);

        Collection<QdbColumn.Definition> columns = new ArrayList<QdbColumn.Definition> ();
        for (qdb_ts_column_info column : nativeColumns.value) {
            columns.add(QdbColumn.Definition.fromNative(column));
        }

        return columns;
    }
}
