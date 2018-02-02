package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.io.IOException;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

final class QdbStreamChannel implements SeekableByteChannel {
    private long handle;

    protected QdbStreamChannel(long handle) {
        this.handle = handle;
    }

    public void close() throws IOException {
        if (handle == 0)
            return;
        int err = qdb.stream_close(handle);
        throwIfError(err);
        handle = 0;
    }

    public boolean isOpen() {
        return handle != 0;
    }

    public long position() throws IOException {
        throwIfClosed();
        Reference<Long> position = new Reference<Long>();
        int err = qdb.stream_getpos(handle, position);
        throwIfError(err);
        return position.value;
    }

    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0)
            throw new IllegalArgumentException("New position must be positive");

        throwIfClosed();
        int err = qdb.stream_setpos(handle, newPosition);
        throwIfError(err);
        return this;
    }

    public int read(ByteBuffer dst) throws IOException {
        if (dst == null)
            throw new IllegalArgumentException("Destination ByteBuffer can not be null");

        throwIfClosed();

        if (dst.remaining() == 0)
            return 0;

        Reference<Long> bytesRead = new Reference<Long>();
        int err = qdb.stream_read(handle, dst.slice(), bytesRead);
        throwIfError(err);

        if (bytesRead.value > 0) {
            dst.position(dst.position() + bytesRead.value.intValue());
            return bytesRead.value.intValue();
        } else {
            return -1;
        }
    }

    public long size() throws IOException {
        throwIfClosed();
        Reference<Long> size = new Reference<Long>();
        int err = qdb.stream_size(handle, size);
        throwIfError(err);
        return size.value;
    }

    public SeekableByteChannel truncate(long size) throws IOException {
        if (size < 0)
            throw new IllegalArgumentException("Size must be positive");

        throwIfClosed();

        int err = qdb.stream_truncate(handle, size);
        if (err == qdb_error.operation_not_permitted)
            throw new NonWritableChannelException();
        if (err != qdb_error.out_of_bounds)
            throwIfError(err);
        return this;
    }

    public int write(ByteBuffer src) throws IOException {
        if (src == null)
            throw new IllegalArgumentException("Source ByteBuffer can not be null");

        throwIfClosed();

        int err = qdb.stream_write(handle, src.slice());
        if (err == qdb_error.operation_not_permitted)
            throw new NonWritableChannelException();

        throwIfError(err);
        src.position(src.limit());
        return src.limit();
    }

    private void throwIfClosed() throws IOException {
        if (handle == 0)
            throw new ClosedChannelException();
    }

    private void throwIfError(int err) throws IOException {
        if (qdb_error.severity(err) == qdb_err_severity.info)
            return;
        throw new IOException(ExceptionFactory.createException(err));
    }
}
