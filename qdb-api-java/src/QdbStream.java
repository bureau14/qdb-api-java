package net.quasardb.qdb;

import java.lang.AutoCloseable;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import net.quasardb.qdb.jni.*;

/**
 * Represents a stream in a quasardb database.
 */
public final class QdbStream extends QdbEntry implements AutoCloseable, SeekableByteChannel {
    protected QdbStream(final SWIGTYPE_p_qdb_session session, final String alias) {
        super(session, alias);
    }

    /**
     * @param option Open mode. Only {@link StandardOpenOption#READ} and {@link StandardOpenOption#APPEND} are supported
     */
    public void open(StandardOpenOption option) {
        error_carrier error = new error_carrier();
        error.setError(qdb_error_t.error_invalid_argument);

        switch (option) {
        case READ:
            stream = qdb.stream_open(this.session, this.alias, qdb_stream_mode_t.qdb_stream_mode_read, error);
            break;
        case APPEND:
            stream = qdb.stream_open(this.session, this.alias, qdb_stream_mode_t.qdb_stream_mode_append, error);
            break;
        // TODO(marek): Should we throw QdbInvalidArgumentException or UnsupportedOperationException?
        }

        QdbExceptionThrower.throwIfError(error);
    }

    public boolean isOpen() {
        return (stream != null);
    }

    public void close() {
        qdb_error_t err = qdb.stream_close(stream);
        QdbExceptionThrower.throwIfError(err);
        stream = null;
    }

    public long position() {
        long[] pos = {0};
        qdb_error_t err = qdb.stream_getpos(stream, pos);
        QdbExceptionThrower.throwIfError(err);
        return pos[0];
    }

    public QdbStream position(long newPosition) {
        qdb_error_t err = qdb.stream_setpos(stream, newPosition);
        QdbExceptionThrower.throwIfError(err);
        return this;
    }

    public QdbStream truncate(long size) {
        qdb_error_t err = qdb.stream_truncate(stream, size);
        QdbExceptionThrower.throwIfError(err);
        return this;
    }

    public long size() {
        long[] sz = {0};
        qdb_error_t err = qdb.stream_size(stream, sz);
        QdbExceptionThrower.throwIfError(err);
        return sz[0];
    }

    public int read(ByteBuffer dst) {
        int[] sz = {dst.limit()};
        qdb_error_t err = qdb.stream_read(stream, dst, sz);
        QdbExceptionThrower.throwIfError(err);
        return sz[0];
    }

    public int write(ByteBuffer content) {
        qdb_error_t err = qdb.stream_write(stream, content, content.limit());
        QdbExceptionThrower.throwIfError(err);
        return content.limit();
    }

    private SWIGTYPE_p_qdb_stream_session stream;
}
