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
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
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

package net.quasardb.qdb.data;

import java.io.Serializable;

public class Email implements Serializable {

    private static final long serialVersionUID = 1L;

    private String _name;
    private String _email;

    public Email() {
    }

    public Email( final String name, final String email ) {
        super();
        _name = name;
        _email = email;
    }

    public String getName() {
        return _name;
    }

    public void setName( final String name ) {
        _name = name;
    }

    public String getEmail() {
        return _email;
    }

    public void setEmail( final String email ) {
        _email = email;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( _email == null )
            ? 0
            : _email.hashCode() );
        result = prime * result + ( ( _name == null )
            ? 0
            : _name.hashCode() );
        return result;
    }

    @Override
    public boolean equals( final Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        final Email other = (Email) obj;
        if ( _email == null ) {
            if ( other._email != null ) {
                return false;
            }
        } else if ( !_email.equals( other._email ) ) {
            return false;
        }
        if ( _name == null ) {
            if ( other._name != null ) {
                return false;
            }
        } else if ( !_name.equals( other._name ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Email [_email=" + _email + ", _name=" + _name + "]";
    }

}
