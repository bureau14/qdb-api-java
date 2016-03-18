package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.io.IOException;
import net.quasardb.qdb.jni.*;

final class QdbStreamChannel implements SeekableByteChannel {
    private SWIGTYPE_p_qdb_stream_session stream;

    protected QdbStreamChannel(SWIGTYPE_p_qdb_stream_session stream) {
        this.stream = stream;
    }

    public void close() throws IOException {
        if (stream == null)
            return;
        qdb_error_t err = qdb.stream_close(stream);
        throwIfError(err);
        stream = null;
    }

    public boolean isOpen() {
        return stream != null;
    }

    public long position() throws IOException {
        throwIfClosed();
        long[] pos = {0};
        qdb_error_t err = qdb.stream_getpos(stream, pos);
        throwIfError(err);
        return pos[0];
    }

    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0)
            throw new IllegalArgumentException("New position must be positive");

        throwIfClosed();
        qdb_error_t err = qdb.stream_setpos(stream, newPosition);
        throwIfError(err);
        return this;
    }

    public int read(ByteBuffer dst) throws IOException {
        if (dst == null)
            throw new IllegalArgumentException("Destination ByteBuffer can not be null");

        throwIfClosed();

        if (dst.remaining() == 0)
            return 0;

        ByteBuffer dstSlice = dst.slice();
        int[] sz = {dstSlice.limit()};
        qdb_error_t err = qdb.stream_read(stream, dstSlice.slice(), sz);
        throwIfError(err);

        if (sz[0] > 0) {
            dst.position(dst.position() + sz[0]);
            return sz[0];
        } else {
            return -1;
        }
    }

    public long size() throws IOException {
        throwIfClosed();
        long[] sz = {0};
        qdb_error_t err = qdb.stream_size(stream, sz);
        throwIfError(err);
        return sz[0];
    }

    public SeekableByteChannel truncate(long size) throws IOException {
        if (size < 0)
            throw new IllegalArgumentException("Size must be positive");

        throwIfClosed();

        qdb_error_t err = qdb.stream_truncate(stream, size);
        if (err == qdb_error_t.error_operation_not_permitted)
            throw new NonWritableChannelException();
        if (err != qdb_error_t.error_out_of_bounds)
            throwIfError(err);
        return this;
    }

    public int write(ByteBuffer src) throws IOException {
        if (src == null)
            throw new IllegalArgumentException("Source ByteBuffer can not be null");

        throwIfClosed();

        qdb_error_t err = qdb.stream_write(stream, src.slice(), src.remaining());
        if (err == qdb_error_t.error_operation_not_permitted)
            throw new NonWritableChannelException();

        throwIfError(err);
        src.position(src.limit());
        return src.limit();
    }

    private void throwIfClosed() throws IOException {
        if (stream == null)
            throw new ClosedChannelException();
    }

    private void throwIfError(qdb_error_t err) throws IOException {
        QdbException exception = QdbExceptionFactory.createException(err);
        if (exception != null)
            throw new IOException(exception);
    }
}
