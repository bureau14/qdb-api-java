package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

final class QdbSession {
    private transient SWIGTYPE_p_qdb_session handle;

    static {
        QdbJniApi.load();
    }

    public QdbSession() {
        handle = qdb.open();
    }

    public void close() {
        if (handle != null) {
            qdb.close(handle);
            handle = null;
        }
    }

    public boolean isClosed() {
        return handle == null;
    }

    public void throwIfClosed() {
        if (handle == null)
            throw new QdbClusterClosedException();
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public SWIGTYPE_p_qdb_session handle() {
        return handle;
    }
}
