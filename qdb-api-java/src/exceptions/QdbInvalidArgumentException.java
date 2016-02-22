package net.quasardb.qdb;

/**
 * Exception thrown when argument passed to a method is incorrect.
 */
public class QdbInvalidArgumentException extends QdbException {

    public QdbInvalidArgumentException() {
        super("The argument is invalid.");
    }

    public QdbInvalidArgumentException(String message) {
        super(message);
    }
}
