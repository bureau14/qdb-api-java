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

        if (isConnectionError(errorCode))
            return createConnectionException(errorCode);

        if (isInputError(errorCode))
            return createInputException(errorCode);

        if (isLocalSystemError(errorCode))
            return createLocalSystemException(errorCode);

        if (isOperationError(errorCode))
            return createOperationException(errorCode);

        if (isRemoteSystemError(errorCode))
            return createRemoteSystemException(errorCode);

        return new QdbException(qdb.make_error_string(errorCode));
    }

    private static boolean isConnectionError(qdb_error_t errorCode) {
        return testErrorOrigin(errorCode, qdb_error_origin_t.error_origin_connection);
    }

    private static boolean isInputError(qdb_error_t errorCode) {
        return testErrorOrigin(errorCode, qdb_error_origin_t.error_origin_input);
    }

    private static boolean isOperationError(qdb_error_t errorCode) {
        return testErrorOrigin(errorCode, qdb_error_origin_t.error_origin_operation);
    }

    private static boolean isLocalSystemError(qdb_error_t errorCode) {
        return testErrorOrigin(errorCode, qdb_error_origin_t.error_origin_system_local);
    }

    private static boolean isRemoteSystemError(qdb_error_t errorCode) {
        return testErrorOrigin(errorCode, qdb_error_origin_t.error_origin_system_remote);
    }

    private static boolean testErrorOrigin(qdb_error_t errorCode, qdb_error_origin_t candidateOrigin) {
        return (errorCode.swigValue() & candidateOrigin.swigValue()) == candidateOrigin.swigValue();
    }

    private static QdbConnectionException createConnectionException(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_connection_refused)
            return new QdbConnectionRefusedException();

        if (errorCode == qdb_error_t.error_host_not_found)
            return new QdbHostNotFoundException();

        return new QdbConnectionException(qdb.make_error_string(errorCode));
    }

    private static QdbInputException createInputException(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_reserved_alias)
            return new QdbReservedAliasException();

        if (errorCode == qdb_error_t.error_invalid_argument)
            return new QdbInvalidArgumentException();

        if (errorCode == qdb_error_t.error_out_of_bounds)
            return new QdbOutOfBoundsException();

        return new QdbInputException(qdb.make_error_string(errorCode));
    }

    private static QdbLocalSystemException createLocalSystemException(qdb_error_t errorCode) {
        return new QdbLocalSystemException(qdb.make_error_string(errorCode));
    }

    private static QdbOperationException createOperationException(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_alias_not_found)
            return new QdbAliasNotFoundException();

        if (errorCode == qdb_error_t.error_alias_already_exists)
            return new QdbAliasAlreadyExistsException();

        if (errorCode == qdb_error_t.error_incompatible_type)
            return new QdbIncompatibleTypeException();

        if (errorCode == qdb_error_t.error_operation_disabled)
            return new QdbOperationDisabledException();

        if (errorCode == qdb_error_t.error_overflow)
            return new QdbOverflowException();

        if (errorCode == qdb_error_t.error_underflow)
            return new QdbUnderflowException();

        if (errorCode == qdb_error_t.error_resource_locked)
            return new QdbResourceLockedException();

        return new QdbOperationException(qdb.make_error_string(errorCode));
    }

    private static QdbRemoteSystemException createRemoteSystemException(qdb_error_t errorCode) {
        if (errorCode == qdb_error_t.error_unexpected_reply)
            return new QdbUnexpectedReplyException();

        return new QdbRemoteSystemException(qdb.make_error_string(errorCode));
    }
}