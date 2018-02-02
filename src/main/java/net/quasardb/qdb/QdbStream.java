package net.quasardb.qdb;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.jni.*;

/**
 * Represents a stream in a quasardb database.
 */
public final class QdbStream extends QdbEntry {

    public enum Mode {
        READ(qdb_stream_mode.read)
        ,
        APPEND(qdb_stream_mode.append);

        protected final int value;
        Mode(int value) {
            this.value = value;
        }
    }

    protected QdbStream(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Opens a ByteChannel for the stream.
     *
     * @param mode How to open the stream: READ or APPEND
     * @return A SeekableByteChannel
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public SeekableByteChannel open(Mode mode) {
        session.throwIfClosed();

        Reference<Long> stream = new Reference<Long>();
        int err = qdb.stream_open(session.handle(), alias, mode.value, stream);
        ExceptionFactory.throwIfError(err);

        return new QdbStreamChannel(stream.value);
    }
}
