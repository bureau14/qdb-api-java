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
package com.b14.qdb.batch;

import java.nio.ByteBuffer;

import com.b14.qdb.batch.TypeOperation;
import com.b14.qdb.batch.TypeOperationMap;
import com.b14.qdb.batch.OperationHasValue;

import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_operation_t;
import com.b14.qdb.jni.qdb_operation_type_t;
import com.b14.qdb.jni.run_batch_result;

/**
 * A Quasardb batch operation result is :
 * <ul>
 * <li>A success status.</li>
 * <li>The alias which was associated with the current operation result.</li>
 * <li>The operation type which was associated with the current operation result.</li>
 * <li>A value which can be the result of the current operation.</li>
 * <li>An error which can be the result of the current operation.</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.0
 */
public class Result {
    private String alias;
    private TypeOperation typeOperation;
    private ByteBuffer value;
    private qdb_error_t error = qdb_error_t.error_uninitialized;
    
    public Result() {
    }

    public Result(qdb_operation_t operation) {
        this.alias = operation.getAlias();
        this.error = operation.getError();

        qdb_operation_type_t op_type = operation.getType();

        this.typeOperation = TypeOperationMap.map(op_type);

        // Set value
        if (OperationHasValue.map(op_type) && this.isSuccess()) {
            final ByteBuffer buffer = operation.getResult();
            if (buffer != null) {
                buffer.rewind();
                this.value = buffer;
            } else {
                this.value = null;
                this.error =  qdb_error_t.error_no_memory;
            }
        }
    }
    
    /**
     * Are this current operation successful ?
     * 
     * @return true if current operation is successful. false in all other cases.
     */
    public boolean isSuccess() {
        return error == qdb_error_t.error_ok;
    }
    
    /**
     * Get alias associated with the current operation.
     * 
     * @return alias associated with the current operation.
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Get operation type associated with the current operation.
     * 
     * @return Operation type associated with the current operation.
     */
    public TypeOperation getTypeOperation() {
        return typeOperation;
    }

    /**
     * Get result value for the current operation.
     * 
     * @return Result value for the current operation.
     */
    public ByteBuffer getValue() {
        return value;
    }

    /**
     * Get result error for the current operation.
     * 
     * @return Result error for the current operation.
     */
    public qdb_error_t getError() {
        return error;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Result [alias=" + alias + ", typeOperation=" + typeOperation + ", value=" + value + ", error=" + error.toString() + "]";
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alias == null) ? 0 : alias.hashCode());
        result = prime * result + error.swigValue();
        result = prime * result + ((typeOperation == null) ? 0 : typeOperation.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Result other = (Result) obj;
        if (alias == null) {
            if (other.alias != null) {
                return false;
            }
        } else if (!alias.equals(other.alias)) {
            return false;
        }
        if (error == null) {
            if (other.error != null) {
                return false;
            }
        } else if (!error.equals(other.error)) {
            return false;
        }
        if (typeOperation != other.typeOperation) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }
}
