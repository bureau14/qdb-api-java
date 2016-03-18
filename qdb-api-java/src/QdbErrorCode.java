package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

final class QdbErrorCode {
    private final qdb_error_t errorCode;

    protected QdbErrorCode(qdb_error_t errorCode) {
        this.errorCode = errorCode;
    }

    public boolean equals(qdb_error_t errorCode) {
        return this.errorCode == errorCode;
    }

    public String message() {
        return qdb.make_error_string(errorCode);
    }

    public qdb_error_origin_t origin() {
        return qdb_error_origin_t.swigToEnum(errorCode.swigValue() & 0xf0000000);
    }

    public qdb_error_severity_t severity() {
        return qdb_error_severity_t.swigToEnum(errorCode.swigValue() & 0x0f000000);
    }
}
