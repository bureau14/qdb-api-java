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

    @Override
    protected void finalize() throws Throwable {
        qdb.close(handle);
        super.finalize();
    }

    public SWIGTYPE_p_qdb_session handle() {
        return handle;
    }
}
