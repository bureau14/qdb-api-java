package net.quasardb.qdb;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;

/**
 * Represents a stream in a quasardb database.
 */
public final class QdbStream extends QdbEntry {

    public enum Mode {
        READ,
        APPEND
    }

    protected QdbStream(QdbSession session, String alias) {
        super(session, alias);
    }

    public SeekableByteChannel open(Mode option) {
        qdb_stream_mode_t jniMode;

        switch (option) {
        case READ:
            jniMode = qdb_stream_mode_t.qdb_stream_mode_read;
            break;
        case APPEND:
            jniMode = qdb_stream_mode_t.qdb_stream_mode_append;
            break;
        default:
            throw new UnsupportedOperationException();
        }

        error_carrier error = new error_carrier();

        SWIGTYPE_p_qdb_stream_session stream = qdb.stream_open(session.handle(), alias, jniMode, error);
        QdbExceptionFactory.throwIfError(error);

        return new QdbStreamChannel(stream);
    }
}
