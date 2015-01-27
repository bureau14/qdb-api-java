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

package com.b14.qdb;

import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * Quasardb exceptions.<br>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 0.5.2
 */
public class QuasardbException extends Exception {

    // Id for serialization
    private static final long serialVersionUID = 2196126450147576611L;

    // The exception message
    private transient String message;
    
    // The exception code
    private transient String code;

    /**
     * Default constructor
     */
    protected QuasardbException() {
        super();
    }

    /**
     * Custom Quasardb message exception
     * 
     * @param message the specific message related to the exception
     */
    protected QuasardbException(final String message) {
        super(message);
        this.code = message;
        this.message = message;
    }
    
    /**
     * Custom Quasardb message exception
     * 
     * @param message the specific message related to the exception
     */
    protected QuasardbException(final String code, final String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    /**
     * Build a QuasardbException using a thrown exception
     * 
     * @param cause the exception to handle
     */
    protected QuasardbException(final Throwable cause) {
        super(cause);
        this.message = cause.getMessage();
        this.code = cause.getClass().toString();
    }
    
    /**
     * Build a QuasardbException from a thrown exception and a custom message
     * 
     * @param message the custom message of exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final String code, final String message, final Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
    }

    /**
     * Build a QuasardbException from a thrown exception and a custom message
     * 
     * @param message the custom message of exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final String message, final Throwable cause) {
        super(message, cause);
        this.message = message;
        this.code = message;
    }
    
    /**
     * Handle all JNI errors by providing a custom QuasardbException
     * 
     * @param jniError the JNI error to handle
     */
    protected QuasardbException(final qdb_error_t jniError) {
        super();
        this.message = qdb.make_error_string(jniError);
        this.code = jniError.toString();
    }
    
    /**
     * Handle all JNI errors with a custom message by providing a custom QuasardbException
     * 
     * @param jniError the JNI error to handle
     * @since 1.1.5
     */
    protected QuasardbException(final String message, final qdb_error_t jniError) {
        super();
        this.message = message + " -> " + qdb.make_error_string(jniError);
        this.code = jniError.toString();
    }

    @Override
    public String toString() {
        return this.code + " : " + this.message;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
    
    /**
     * Get error code of Exception
     * 
     * @return error code
     */
    public String getCode() {
        return this.code;
    }
}
