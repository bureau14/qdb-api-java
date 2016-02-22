package net.quasardb.qdb;

/**
 * Exception thrown when the operation cannot be performed because the 64-bit integer would underflow
 */
public class QdbUnderflowException extends QdbException {

    public QdbUnderflowException() {
        super("The operation provokes underflow.");
    }
}
