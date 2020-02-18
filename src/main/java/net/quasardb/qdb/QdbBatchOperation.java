package net.quasardb.qdb;

abstract class QdbBatchOperation {
    public abstract void write(long handle, long batch, int index);
    public abstract void read(long handle, long batch, int index);
    public Object result;
    public int error;
}
