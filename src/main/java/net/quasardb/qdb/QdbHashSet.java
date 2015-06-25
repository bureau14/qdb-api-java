package net.quasardb.qdb;

import java.nio.ByteBuffer;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

import net.quasardb.qdb.QdbEntry;

public class QdbHashSet extends QdbEntry{

    public QdbHashSet(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * 
     * @since 2.0.0
     */
    public boolean contains(ByteBuffer e) throws QdbException {
        final qdb_error_t err = qdb.hset_contains(session, getAlias(), e, e.limit());
        if (err == qdb_error_t.error_element_not_found)
        {
            return false;
        }

        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }

        return true;
    }

    /**
     * 
     * @since 2.0.0
     */
    public boolean insert(ByteBuffer e) throws QdbException {
        final qdb_error_t err = qdb.hset_insert(session, getAlias(), e, e.limit());
        if (err == qdb_error_t.error_element_already_exists)
        {
            return false;
        }

        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }

        return true;
    }

    /**
     * 
     * @since 2.0.0
     */
    public boolean erase(ByteBuffer e) throws QdbException {
        final qdb_error_t err = qdb.hset_erase(session, getAlias(), e, e.limit());
        if (err == qdb_error_t.error_element_not_found)
        {
            return false;
        }

        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }

        return true;
    }


}
