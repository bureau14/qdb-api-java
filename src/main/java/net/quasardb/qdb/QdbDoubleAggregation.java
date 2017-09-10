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

    public static qdb_ts_double_aggregation toNative(QdbDoubleAggregation input) {
        return null;
    }

}
