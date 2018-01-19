package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.FilteredRange;
import net.quasardb.qdb.jni.*;

public class QdbBlobAggregation extends QdbAggregation<QdbBlobColumnValue> {

    public QdbBlobAggregation (Type type, TimeRange range) {
        super(type, range, 0, new QdbBlobColumnValue());
    }


    public QdbBlobAggregation (Type type, TimeRange range, long count, QdbBlobColumnValue value) {
        super(type, range, count, value);
    }

    /**
     * Converts this object to JNI-compatible representation.
     */
    protected static qdb_ts_blob_aggregation toNative(QdbBlobAggregation input) {
        return new qdb_ts_blob_aggregation (new FilteredRange(input.range),
                                            input.type.value,
                                            input.count,
                                            QdbBlobColumnValue.toNative(input.result));
    }

    /**
     * Creates this object from JNI-compatible representation.
     */
    protected static QdbBlobAggregation fromNative(qdb_ts_blob_aggregation input) {
        // :TODO: implement actual filtered ranges here, we're assuming everything is 'no filter' here.
        assert(input.getFilteredRange().getFilter().getType() == 0);

        return new QdbBlobAggregation(Type.values()[(int)input.getAggregationType()],
                                      input.getFilteredRange().getRange(),
                                      input.getCount(),
                                      QdbBlobColumnValue.fromNative(input.getResult()));
    }
}
