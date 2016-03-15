package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

class QdbExceptionFactory {

    public static void throwIfError(error_carrier errorCarrier) {
        throwIfError(errorCarrier.getError());
    }

    public static void throwIfError(qdb_error_t errorCode) {
        QdbException exception = createException(errorCode);
        if (exception != null)
            throw exception;
    }

    static QdbException createException(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_ok)
            return null;

        if (errorCode == qdb_error_t.error_alias_not_found)
            return new QdbAliasNotFoundException();

        if (errorCode == qdb_error_t.error_alias_already_exists)
            return new QdbAliasAlreadyExistsException();

        if (errorCode == qdb_error_t.error_incompatible_type)
            return new QdbIncompatibleTypeException();

        if (errorCode == qdb_error_t.error_reserved_alias)
            return new QdbReservedAliasException();

        if (errorCode == qdb_error_t.error_operation_disabled)
            return new QdbOperationDisabledException();

        if (errorCode == qdb_error_t.error_invalid_argument)
            return new QdbInvalidArgumentException();

        if (errorCode == qdb_error_t.error_overflow)
            return new QdbOverflowException();

        if (errorCode == qdb_error_t.error_underflow)
            return new QdbUnderflowException();

        if (errorCode == qdb_error_t.error_out_of_bounds)
            return new QdbOutOfBoundsException();

        if (errorCode == qdb_error_t.error_unexpected_reply)
            return new QdbUnexpectedReplyException();

        if (errorCode == qdb_error_t.error_connection_refused)
            return new QdbConnectionRefusedException();

        if (errorCode == qdb_error_t.error_host_not_found)
            return new QdbHostNotFoundException();

        if (errorCode == qdb_error_t.error_resource_locked)
            return new QdbResourceLockedException();

        return new QdbMiscException(errorCode);
    }
}