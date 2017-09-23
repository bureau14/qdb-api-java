package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;
import java.util.stream.Collectors;

public class QdbDoubleAggregation extends QdbAggregation<QdbDoubleColumnValue> {

    public QdbDoubleAggregation (Type type, QdbTimeRange range) {
        super(type, range, 0, new QdbDoubleColumnValue());
    }

    public QdbDoubleAggregation (Type type, QdbTimeRange range, long count, QdbDoubleColumnValue value) {
        super(type, range, count, value);
    }

    /**
     * Converts this object to JNI-compatible representation.
     */
    protected static qdb_ts_double_aggregation toNative(QdbDoubleAggregation input) {
        // :TOOD: implement actual filtered ranges, we're just default to 'no filter' here.
        return new qdb_ts_double_aggregation (QdbTimeRange.toNative(input.range),
                                              input.type.value,
                                              input.count,
                                              QdbDoubleColumnValue.toNative(input.result));
    }

    /**
     * Creates this object from JNI-compatible representation.
     */
    protected static QdbDoubleAggregation fromNative(qdb_ts_double_aggregation input) {
        // :TODO: implement actual filtered ranges here, we're assuming everything is 'no filter' here.
        assert(input.getFilteredRange().getFilter().getType() == 0);

        return new QdbDoubleAggregation(Type.values()[(int)input.getAggregationType()],
                                        QdbTimeRange.fromNative(input.getFilteredRange()),
                                        input.getCount(),
                                        QdbDoubleColumnValue.fromNative(input.getResult()));
    }
}
