package net.quasardb.qdb;

/**
 * Exception thrown when an index is out of range.
 */
public class QdbOutOfBoundsException extends QdbException {

    public QdbOutOfBoundsException() {
        super("The given index was out of bounds.");
    }
}
