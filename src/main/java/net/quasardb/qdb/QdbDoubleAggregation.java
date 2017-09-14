package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;
import java.util.stream.Collectors;

public class QdbDoubleAggregation extends QdbAggregation {

    public QdbDoubleAggregation (Type type, QdbTimeRange range) {
        super(type, range);
    }


    public QdbDoubleAggregation (Type type, QdbTimeRange range, long count, QdbDoubleColumnValue value) {
        super(type, range, count, value);
    }

    /**
     * Converts this object to JNI-compatible representation.
     */
    protected static qdb_ts_double_aggregation toNative(QdbDoubleAggregation input) {
        return new qdb_ts_double_aggregation (QdbTimeRange.toNative(input.range),
                                              input.type.value,
                                              input.count,
                                              QdbDoubleColumnValue.toNative(input.result));
    }

    /**
     * Creates this object from JNI-compatible representation.
     */
    protected static QdbDoubleAggregation fromNative(qdb_ts_double_aggregation input) {
        System.out.println("converting double aggregation, count: " + input.getCount() + ", value: " + input.getResult());
        return new QdbDoubleAggregation(Type.values()[(int)input.getAggregationType()],
                                        QdbTimeRange.fromNative(input.getRange()),
                                        input.getCount(),
                                        QdbDoubleColumnValue.fromNative(input.getResult()));
    }
}
