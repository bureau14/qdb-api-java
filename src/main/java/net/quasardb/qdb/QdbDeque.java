package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A deque in the database.
 * Deque stands for "double-ended queue", you can both enqueue and dequeue from the front and the back.
 */
public final class QdbDeque extends QdbEntry {
    // Protected constructor. Call QdbCluster.deque() to get an instance.
    protected QdbDeque(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Retrieves the item at the back of the queue. The queue must already exist.
     *
     * @return The content of the item, or null if the deque was empty.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer back() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.deque_back(session.handle(), alias, content);
        if (err == qdb_error.container_empty)
            return null;
        ExceptionFactory.throwIfError(err);
        return Buffer.wrap(session, content);
    }

    /**
     * Retrieves the value of the queue at the specified index. The queue must already exist.
     *
     * @param index The zero-based index you wish to retrieve.
     * @return The content of the item.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws OutOfBoundsException If the index is negative, or greater or equal than deque size.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer get(long index) {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.deque_get_at(session.handle(), alias, index, content);
        ExceptionFactory.throwIfError(err);
        return Buffer.wrap(session, content);
    }

    /**
     * Retrieves the item at the front of the queue. The queue must already exist.
     *
     * @return The content of the item, or null if the deque was empty.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer front() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.deque_front(session.handle(), alias, content);
        if (err == qdb_error.container_empty)
            return null;
        ExceptionFactory.throwIfError(err);
        return Buffer.wrap(session, content);
    }

    /**
     * Removes and retrieves the item at the back of the queue. The queue must already exist.
     *
     * @return The content of the removed item, or null if the deque was empty.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer popBack() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.deque_pop_back(session.handle(), alias, content);
        if (err == qdb_error.container_empty)
            return null;
        ExceptionFactory.throwIfError(err);
        return Buffer.wrap(session, content);
    }

    /**
     * Removes and retrieves the item at the front of the queue. The queue must already exist.
     *
     * @return The content of the removed item, or null if the deque was empty.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer popFront() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.deque_pop_front(session.handle(), alias, content);
        if (err == qdb_error.container_empty)
            return null;
        ExceptionFactory.throwIfError(err);
        return Buffer.wrap(session, content);
    }

    /**
     * Inserts the content at the back of the queue. Creates the queue if it does not exist.
     *
     * @param content The content that will be added to the queue.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void pushBack(ByteBuffer content) {
        session.throwIfClosed();
        int err = qdb.deque_push_back(session.handle(), alias, content);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Inserts the content at the front of the queue. Creates the queue if it does not exist.
     *
     * @param content The content that will be added to the queue.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void pushFront(ByteBuffer content) {
        session.throwIfClosed();
        int err = qdb.deque_push_front(session.handle(), alias, content);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Retrieves the size of the queue. The queue must already exist.
     *
     * @return The number of items in the deque.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long size() {
        session.throwIfClosed();
        Reference<Long> size = new Reference<Long>();
        int err = qdb.deque_size(session.handle(), alias, size);
        ExceptionFactory.throwIfError(err);
        return size.value.longValue();
    }
}
