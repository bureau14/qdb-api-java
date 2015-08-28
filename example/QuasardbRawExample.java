/**
 * Copyright (c) 2009-2015, quasardb SAS
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
 
import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.tools.LibraryHelper;

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
            qdb_error_t r = qdb.connect(session, "127.0.0.1", 2836);
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

            r = qdb.blob_(session, key, bb, bb.limit(), 0);
            if (r != qdb_error_t.error_ok) {
                System.err.println("Cannot add: " + r);
            }

            String myNewData = "this is my new data";
            bb.clear();
            bb.put(myNewData.getBytes());
            bb.flip();

            r = qdb.blob_put(session, key, bb, bb.limit(), 0);
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
