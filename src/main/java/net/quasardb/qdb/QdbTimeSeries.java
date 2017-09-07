package net.quasardb.qdb;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries inside quasardb
 */
public final class QdbTimeSeries {

    public enum ColumnType {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int type;
        ColumnType(int type) {
            this.type = type;
        }
    }

    /**
     * Describes a time-series column, required to create the column.
     */
    public static class ColumnDefinition {
        String name;
        ColumnType type;

        ColumnDefinition(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }
    }

    /**
     * Describes a time-series column that contains blobs
     */
    public static class BlobColumnDefinition extends ColumnDefinition {
        BlobColumnDefinition(String name) {
            super(name, ColumnType.BLOB);
        }
    }

    /**
     * Describes a time-series column that contains double precision floating point values
     */
    public static class DoubleColumnDefinition extends ColumnDefinition {
        DoubleColumnDefinition(String name) {
            super(name, ColumnType.DOUBLE);
        }
    }


    /**
     * A collection of columns.
     */
    static class ColumnCollection {
    }

}
