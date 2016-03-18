package net.quasardb.qdb;

public interface QdbFuture<T> {
    T get();
    boolean success();
}
