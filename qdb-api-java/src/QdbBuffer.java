package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

public final class QdbBuffer {
    QdbSession session;
    ByteBuffer buffer;

    protected QdbBuffer(QdbSession session, ByteBuffer buffer) {
        this.session = session;
        this.buffer = buffer;
    }

    @Override
    protected void finalize() throws Throwable {
        qdb.free_buffer(session.handle(), buffer);
        super.finalize();
    }

    public void free() {
        qdb.free_buffer(session.handle(), buffer);
        buffer = null;
    }

    public ByteBuffer toByteBuffer() {
        return buffer != null ? buffer.duplicate() : null;
    }

    @Override
    public String toString() {
        if (buffer == null)
            return this.getClass().getSimpleName() + "[freed]";
        else
            return this.getClass().getName() + "[size=" + buffer.limit() + "]";
    }
}
