package net.quasardb.qdb;

public class NativeLibraryLoadError extends RuntimeException {
    public NativeLibraryLoadError(String message, Throwable cause) {
        super(message, cause);
    }

    public NativeLibraryLoadError(String message) {
        super(message);
    }
}
