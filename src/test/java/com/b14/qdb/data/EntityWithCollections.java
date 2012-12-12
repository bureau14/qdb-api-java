/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
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
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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

package com.b14.qdb.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityWithCollections {
    private final String[] _bars;
    private final List<String> _foos;
    private final Map<String, Integer> _bazens;

    public EntityWithCollections() {
        _bars = new String[] { "foo", "bar" };
        _foos = new ArrayList<String>( Arrays.asList( "foo", "bar" ) );
        _bazens = new HashMap<String, Integer>();
        _bazens.put( "foo", 1 );
        _bazens.put( "bar", 2 );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode( _bars );
        result = prime * result + ( ( _bazens == null )
            ? 0
            : _bazens.hashCode() );
        result = prime * result + ( ( _foos == null )
            ? 0
            : _foos.hashCode() );
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
        final EntityWithCollections other = (EntityWithCollections) obj;
        if ( !Arrays.equals( _bars, other._bars ) ) {
            return false;
        }
        if ( _bazens == null ) {
            if ( other._bazens != null ) {
                return false;
            }
        } else if ( !_bazens.equals( other._bazens ) ) {
            return false;
        }
        if ( _foos == null ) {
            if ( other._foos != null ) {
                return false;
            }
        } else if ( !_foos.equals( other._foos ) ) {
            return false;
        }
        return true;
    }
}
