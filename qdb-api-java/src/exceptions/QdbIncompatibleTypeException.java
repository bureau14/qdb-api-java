package net.quasardb.qdb;

/**
 * Exception thrown when the specified entry has a type incompatible for this operation.
 */
public class QdbIncompatibleTypeException extends QdbException {

    public QdbIncompatibleTypeException() {
        super("The alias has a type incompatible for this operation.");
    }
}
