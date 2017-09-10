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

    /**
     * Converts this object to JNI-compatible representation.
     */
    protected static qdb_ts_double_aggregation toNative(QdbDoubleAggregation input) {
        return new qdb_ts_double_aggregation (QdbTimeRange.toNative(input.range),
                                              input.type.value,
                                              input.count,
                                              QdbDoubleColumnValue.toNative(input.result));
    }

}
