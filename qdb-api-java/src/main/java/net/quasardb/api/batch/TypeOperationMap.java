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
package net.quasardb.qdb.batch;

import net.quasardb.qdb.batch.TypeOperation;
import net.quasardb.qdb.jni.qdb_operation_type_t;

public class TypeOperationMap
{

    static private TypeOperation[] theMap =
    {
        TypeOperation.GET,        // qdb_op_get = 0,
        TypeOperation.PUT,        // qdb_op_put = 1,
        TypeOperation.UPDATE,     // qdb_op_update = 2,
        TypeOperation.REMOVE,     // qdb_op_remove = 3,
        TypeOperation.CAS,        // qdb_op_cas = 4,
        TypeOperation.GET_UPDATE, // qdb_op_get_and_update = 5,
        TypeOperation.GET_REMOVE, // qdb_op_get_and_remove = 6,
        TypeOperation.REMOVE_IF,  // qdb_op_remove_if = 7
    };

    static public TypeOperation map(qdb_operation_type_t v)
    {
        return theMap[v.swigValue()];
    }

};
