package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.FilteredRange;
import net.quasardb.qdb.jni.*;

public class QdbDoubleAggregation extends QdbAggregation<QdbDoubleColumnValue> {

    public QdbDoubleAggregation (Type type, TimeRange range) {
        super(type, range, 0, new QdbDoubleColumnValue());
    }

    public QdbDoubleAggregation (Type type, TimeRange range, long count, QdbDoubleColumnValue value) {
        super(type, range, count, value);
    }

    /**
     * Converts this object to JNI-compatible representation.
     */
    protected static qdb_ts_double_aggregation toNative(QdbDoubleAggregation input) {
        // :TODO: implement actual filtered ranges here, we're assuming everything is 'no filter' here.
        return new qdb_ts_double_aggregation (new FilteredRange(input.range),
                                              input.type.value,
                                              input.count,
                                              QdbDoubleColumnValue.toNative(input.result));
    }

    /**
     * Creates this object from JNI-compatible representation.
     */
    protected static QdbDoubleAggregation fromNative(qdb_ts_double_aggregation input) {
        assert(input.getFilteredRange().getFilter().getType() == 0);

        return new QdbDoubleAggregation(Type.values()[(int)input.getAggregationType()],
                                        input.getFilteredRange().getRange(),
                                        input.getCount(),
                                        QdbDoubleColumnValue.fromNative(input.getResult()));
    }
}
