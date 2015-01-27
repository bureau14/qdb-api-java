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

package com.b14.qdb.data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("unused")
public class MyContainer {

    private int _int;
    private long _long;
    private final boolean _boolean;
    private final Boolean _Boolean;
    private final Class<?> _Class;
    private String _String;
    private final StringBuilder _StringBuilder;
    private final StringBuffer _StringBuffer;
    private Long _Long;
    private Integer _Integer;
    private Character _Character;
    private Byte _Byte;
    private Double _Double;
    private Float _Float;
    private Short _Short;
    private BigDecimal _BigDecimal;
    private AtomicInteger _AtomicInteger;
    private AtomicLong _AtomicLong;
    private Integer[] _IntegerArray;
    private Date _Date;
    private Calendar _Calendar;
    private final Currency _Currency;
    private List<String> _ArrayList;
    private final Set<String> _HashSet;
    private final Map<String, Integer> _HashMap;
    private int[] _intArray;
    private long[] _longArray;
    private short[] _shortArray;
    private float[] _floatArray;
    private double[] _doubleArray;
    private byte[] _byteArray;
    private char[] _charArray;
    private String[] _StringArray;
    private Person[] _PersonArray;

    public MyContainer() {

        _int = 1;
        _long = 2;
        _boolean = true;
        _Boolean = Boolean.TRUE;
        _Class = String.class;
        _String = "3";
        _StringBuffer = new StringBuffer( "foo" );
        _StringBuilder = new StringBuilder( "foo" );
        _Long = new Long( 4 );
        _Integer = new Integer( 5 );
        _Character = new Character( 'c' );
        _Byte = new Byte( "b".getBytes()[0] );
        _Double = new Double( 6d );
        _Float = new Float( 7f );
        _Short = new Short( (short) 8 );
        _BigDecimal = new BigDecimal( 9 );
        _AtomicInteger = new AtomicInteger( 10 );
        _AtomicLong = new AtomicLong( 11 );
        _IntegerArray = new Integer[] { 13 };
        _Date = new Date( System.currentTimeMillis() - 10000 );
        _Calendar = Calendar.getInstance();
        _Currency = Currency.getInstance( "EUR" );
        _ArrayList = new ArrayList<String>( Arrays.asList( "foo" ) );
        _HashSet = new HashSet<String>();
        _HashSet.add( "14" );

        _HashMap = new HashMap<String, Integer>();
        _HashMap.put( "foo", 23 );
        _HashMap.put( "bar", 42 );

        _intArray = new int[] { 1, 2 };
        _longArray = new long[] { 1, 2 };
        _shortArray = new short[] { 1, 2 };
        _floatArray = new float[] { 1, 2 };
        _doubleArray = new double[] { 1, 2 };
        _byteArray = "42".getBytes();
        _charArray = "42".toCharArray();
        _StringArray = new String[] { "23", "42" };
        _PersonArray = new Person[] { new Person( "foo bar", Gender.MALE, 42 ) };

    }

    public int getInt() {
        return _int;
    }

    public void setInt( final int i ) {
        _int = i;
    }

    public long getLong() {
        return _long;
    }

    public void setLong( final long l ) {
        _long = l;
    }

    public String getString() {
        return _String;
    }

    public void setString( final String string ) {
        _String = string;
    }

    public Long getLongWrapper() {
        return _Long;
    }

    public void setLongWrapper( final Long l ) {
        _Long = l;
    }

    public Integer getInteger() {
        return _Integer;
    }

    public void setInteger( final Integer integer ) {
        _Integer = integer;
    }

    public Character getCharacter() {
        return _Character;
    }

    public void setCharacter( final Character character ) {
        _Character = character;
    }

    public Byte getByte() {
        return _Byte;
    }

    public void setByte( final Byte b ) {
        _Byte = b;
    }

    public Double getDouble() {
        return _Double;
    }

    public void setDouble( final Double d ) {
        _Double = d;
    }

    public Float getFloat() {
        return _Float;
    }

    public void setFloat( final Float f ) {
        _Float = f;
    }

    public Short getShort() {
        return _Short;
    }

    public void setShort( final Short s ) {
        _Short = s;
    }

    public BigDecimal getBigDecimal() {
        return _BigDecimal;
    }

    public void setBigDecimal( final BigDecimal bigDecimal ) {
        _BigDecimal = bigDecimal;
    }

    public AtomicInteger getAtomicInteger() {
        return _AtomicInteger;
    }

    public void setAtomicInteger( final AtomicInteger atomicInteger ) {
        _AtomicInteger = atomicInteger;
    }

    public AtomicLong getAtomicLong() {
        return _AtomicLong;
    }

    public void setAtomicLong( final AtomicLong atomicLong ) {
        _AtomicLong = atomicLong;
    }

    public Integer[] getIntegerArray() {
        return _IntegerArray;
    }

    public void setIntegerArray( final Integer[] integerArray ) {
        _IntegerArray = integerArray;
    }

    public Date getDate() {
        return _Date;
    }

    public void setDate( final Date date ) {
        _Date = date;
    }

    public Calendar getCalendar() {
        return _Calendar;
    }

    public void setCalendar( final Calendar calendar ) {
        _Calendar = calendar;
    }

    public List<String> getArrayList() {
        return _ArrayList;
    }

    public void setArrayList( final List<String> arrayList ) {
        _ArrayList = arrayList;
    }

    public int[] getIntArray() {
        return _intArray;
    }

    public void setIntArray( final int[] intArray ) {
        _intArray = intArray;
    }

    public long[] getLongArray() {
        return _longArray;
    }

    public void setLongArray( final long[] longArray ) {
        _longArray = longArray;
    }

    public short[] getShortArray() {
        return _shortArray;
    }

    public void setShortArray( final short[] shortArray ) {
        _shortArray = shortArray;
    }

    public float[] getFloatArray() {
        return _floatArray;
    }

    public void setFloatArray( final float[] floatArray ) {
        _floatArray = floatArray;
    }

    public double[] getDoubleArray() {
        return _doubleArray;
    }

    public void setDoubleArray( final double[] doubleArray ) {
        _doubleArray = doubleArray;
    }

    public byte[] getByteArray() {
        return _byteArray;
    }

    public void setByteArray( final byte[] byteArray ) {
        _byteArray = byteArray;
    }

    public char[] getCharArray() {
        return _charArray;
    }

    public void setCharArray( final char[] charArray ) {
        _charArray = charArray;
    }

    public String[] getStringArray() {
        return _StringArray;
    }

    public void setStringArray( final String[] stringArray ) {
        _StringArray = stringArray;
    }

    public Person[] getPersonArray() {
        return _PersonArray;
    }

    public void setPersonArray( final Person[] personArray ) {
        _PersonArray = personArray;
    }

    public Set<String> getHashSet() {
        return _HashSet;
    }

    public Map<String, Integer> getHashMap() {
        return _HashMap;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MyContainer other = (MyContainer) obj;
        if (_int !=(other._int)) {
            return false;
        }
        if (_long !=(other._long)) {
            return false;
        }
        if (_boolean !=(other._boolean)) {
            return false;
        }
        if (_Boolean == null) {
            if (other._Boolean != null) {
                return false;
            }
        } else if (!_Boolean.equals(other._Boolean)) {
            return false;
        }
        if (_Class == null) {
            if (other._Class != null) {
                return false;
            }
        } else if (!_Class.getName().equals(other._Class.getName())) {
            return false;
        }
        if (_String == null) {
            if (other._String != null) {
                return false;
            }
        } else if (!_String.equals(other._String)) {
            return false;
        }
        if (_StringBuilder == null) {
            if (other._StringBuilder != null) {
                return false;
            }
        } else if (!_StringBuilder.toString().equals(other._StringBuilder.toString())) {
            return false;
        }
        if (_StringBuffer == null) {
            if (other._StringBuffer != null) {
                return false;
            }
        } else if (!_StringBuffer.toString().equals(other._StringBuffer.toString())) {
            return false;
        }
        if (_Long == null) {
            if (other._Long != null) {
                return false;
            }
        } else if (!_Long.equals(other._Long)) {
            return false;
        }
        if (_Integer == null) {
            if (other._Integer != null) {
                return false;
            }
        } else if (!_Integer.equals(other._Integer)) {
            return false;
        }
        if (_Character == null) {
            if (other._Character != null) {
                return false;
            }
        } else if (!_Character.equals(other._Character)) {
            return false;
        }
        if (_Byte == null) {
            if (other._Byte != null) {
                return false;
            }
        } else if (!_Byte.equals(other._Byte)) {
            return false;
        }
        if (_Double == null) {
            if (other._Double != null) {
                return false;
            }
        } else if (!_Double.equals(other._Double)) {
            return false;
        }
        if (_Float == null) {
            if (other._Float != null) {
                return false;
            }
        } else if (!_Float.equals(other._Float)) {
            return false;
        }
        if (_Short == null) {
            if (other._Short != null) {
                return false;
            }
        } else if (!_Short.equals(other._Short)) {
            return false;
        }
        if (_BigDecimal == null) {
            if (other._BigDecimal != null) {
                return false;
            }
        } else if (_BigDecimal.byteValue() != other._BigDecimal.byteValue()) {
            return false;
        }
        if (_AtomicInteger == null) {
            if (other._AtomicInteger != null) {
                return false;
            }
        } else if (!_AtomicInteger.toString().equals(other._AtomicInteger.toString())) {
            return false;
        }
        if (_AtomicLong == null) {
            if (other._AtomicLong != null) {
                return false;
            }
        } else if (!_AtomicLong.toString().equals(other._AtomicLong.toString())) {
            return false;
        }
        return true;
    }
}
