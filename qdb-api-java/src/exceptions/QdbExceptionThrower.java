package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

class QdbExceptionThrower {

    public static void throwIfError(error_carrier errorCarrier) {
        throwIfError(errorCarrier.getError());
    }

    public static void throwIfError(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_ok)
            return;

        if (errorCode == qdb_error_t.error_alias_not_found)
            throw new QdbAliasNotFoundException();

        if (errorCode == qdb_error_t.error_alias_already_exists)
            throw new QdbAliasAlreadyExistsException();

        if (errorCode == qdb_error_t.error_incompatible_type)
            throw new QdbIncompatibleTypeException();

        if (errorCode == qdb_error_t.error_reserved_alias)
            throw new QdbReservedAliasException();

        if (errorCode == qdb_error_t.error_operation_disabled)
            throw new QdbOperationDisabledException();

        if (errorCode == qdb_error_t.error_invalid_argument)
            throw new QdbInvalidArgumentException();

        if (errorCode == qdb_error_t.error_overflow)
            throw new QdbOverflowException();

        if (errorCode == qdb_error_t.error_underflow)
            throw new QdbUnderflowException();

        if (errorCode == qdb_error_t.error_unexpected_reply)
            throw new QdbUnexpectedReplyException();

        if (errorCode == qdb_error_t.error_connection_refused)
            throw new QdbConnectionRefusedException();

        if (errorCode == qdb_error_t.error_host_not_found)
            throw new QdbHostNotFoundException();

        throw new QdbMiscException(errorCode);
    }
}