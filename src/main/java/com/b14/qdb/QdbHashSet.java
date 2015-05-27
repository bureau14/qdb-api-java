package com.b14.qdb;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

public class QdbHashSet implements Set<ByteBuffer> {
    private final transient SWIGTYPE_p_qdb_session session;
    private final String alias;
    
    public QdbHashSet(SWIGTYPE_p_qdb_session session, String alias) {
        this.session = session;
        this.alias = alias;
    }

    /**
     * Gets the alias (i.e. its "key") of the integer in the database.
     * 
     * @return The alias.
     */
    public final String alias() {
        return this.alias;
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        ByteBuffer e = null;
        try {
            e = ByteBuffer.class.cast(o);
        } catch (ClassCastException ex) {
            return false;
        }
        if (e != null) {
            final qdb_error_t qdbError = qdb.hset_contains(session, alias, e, e.limit());
            if (qdbError != qdb_error_t.error_ok) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public Iterator<ByteBuffer> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean add(ByteBuffer e) {
        if (e == null) {
            return false;
        }
        final qdb_error_t qdbError = qdb.hset_insert(session, alias, e, e.limit());
        if (qdbError != qdb_error_t.error_ok) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        ByteBuffer e = null;
        try {
            e = ByteBuffer.class.cast(o);
        } catch (ClassCastException ex) {
            return false;
        }
        if (e != null) {
            final qdb_error_t qdbError = qdb.hset_erase(session, alias, e, e.limit());
            if (qdbError != qdb_error_t.error_ok) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean containsAll(Collection<?> c) {
        boolean result = false;
        for (Object o : c) {
            if (contains(o) && !result) {
                result = true;
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean addAll(Collection<? extends ByteBuffer> c) {
        boolean result = false;
        if ((c == null) || c.isEmpty()) {
            return result;
        } else {
            for (ByteBuffer b : c) {
                if (add(b) && !result) {
                    result = true;
                }
            }
            return result;
        }
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public boolean removeAll(Collection<?> c) {
        boolean result = false;
        for (Object o : c) {
            if (remove(o) && !result) {
                result = true;
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * @since 2.0.0
     */
    public void clear() {
        qdb.remove(session, alias);
    }

}
