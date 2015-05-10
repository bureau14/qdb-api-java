package com.b14.qdb.examples;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;

public class QuasardbRawExample {
    static {
        try {
            System.loadLibrary("qdb_java_api");
        } catch (UnsatisfiedLinkError e) {
            LibraryHelper.loadLibrairiesFromJar();
        }
    }

    public static void main(String argv[]) {
        SWIGTYPE_p_qdb_session session = null;
        
        try {
            session = qdb.open();
            qdb_error_t r = qdb.connect(session, "qdb://127.0.0.1:2836");
            String key = "myKey";
            if (r != qdb_error_t.error_ok) {
                System.err.println("An error occured: " + r);
            }
            r = qdb.remove(session, key);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot delete: " + r);
            }
            String data= "myData";
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocateDirect(1024);
            bb.put(data.getBytes());
            bb.flip();

            r = qdb.put(session, key, bb, bb.limit(), 0);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot add: " + r);
            }

            String myNewData = "this is my new data";
            bb.clear();
            bb.put(myNewData.getBytes());
            bb.flip();

            r = qdb.put(session, key, bb, bb.limit(), 0);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot add: " + r);
            }
            
            r = qdb.update(session, key, bb, bb.limit(), 0);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot update: " + r);
            }

            error_carrier error = new error_carrier();
            java.nio.ByteBuffer content = qdb.get(session, key, error);
            int [] contentLength = { 0 };
            if (content == null) {
                System.err.println("Cannot get: " + r + "(size = " + contentLength[0] + ")");
            } else {
                System.out.println("Content length: " + contentLength[0]);
                byte [] localBuf = new byte[contentLength[0]];
                content.get(localBuf, 0, contentLength[0]);
                System.out.println("Content of " + key + ": " + new String(localBuf));
            }

            r = qdb.remove(session, key);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot delete: " + r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (session != null) {
                qdb.close(session);
            }
        }
    }
}
