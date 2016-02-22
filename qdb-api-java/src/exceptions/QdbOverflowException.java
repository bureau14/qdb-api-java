package net.quasardb.qdb;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would overflow
 */
public class QdbOverflowException extends QdbException {

    public QdbOverflowException() {
        super("The operation provokes overflow.");
    }
}
