package net.quasardb.qdb;

/**
 * Exception thrown when the operation cannot be performed because it has been disabled.
 */
public class QdbOperationDisabledException extends QdbException {

    public QdbOperationDisabledException() {
        super("The requested operation cannot be performed because it has been disabled.");
    }
}
