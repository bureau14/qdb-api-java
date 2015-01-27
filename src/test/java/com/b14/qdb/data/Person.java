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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    private String _name;
    private Gender _gender;
    private Integer _age;
    private Map<String, Object> _props;
    private final Collection<Person> _friends = new ArrayList<Person>();

    public Person(String name, Gender male, int age) {
        this._name = name;
        this._gender = male;
        this._age = new Integer(age);
    }

    public String getName() {
        return _name;
    }

    public void addFriend( final Person p ) {
        _friends.add( p );
    }

    public void setName( final String name ) {
        _name = name;
    }

    public Map<String, Object> getProps() {
        return _props;
    }

    public void setProps( final Map<String, Object> props ) {
        _props = props;
    }

    public Gender getGender() {
        return _gender;
    }

    public void setGender( final Gender gender ) {
        _gender = gender;
    }

    public Integer getAge() {
        return _age;
    }

    public void setAge( final Integer age ) {
        _age = age;
    }

    public Collection<Person> getFriends() {
        return _friends;
    }

    private boolean flatEquals( final Collection<?> c1, final Collection<?> c2 ) {
        return c1 == c2 || c1 != null && c2 != null && c1.size() == c2.size();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( _age == null )
            ? 0
            : _age.hashCode() );
        result = prime * result + ( ( _friends == null )
            ? 0
            : _friends.size() );
        result = prime * result + ( ( _gender == null )
            ? 0
            : _gender.hashCode() );
        result = prime * result + ( ( _name == null )
            ? 0
            : _name.hashCode() );
        result = prime * result + ( ( _props == null )
            ? 0
            : _props.hashCode() );
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
        final Person other = (Person) obj;
        if ( _age == null ) {
            if ( other._age != null ) {
                return false;
            }
        } else if ( !_age.equals( other._age ) ) {
            return false;
        }
        if ( _friends == null ) {
            if ( other._friends != null ) {
                return false;
            }
        } else if ( !flatEquals( _friends, other._friends ) ) {
            return false;
        }
        if ( _gender == null ) {
            if ( other._gender != null ) {
                return false;
            }
        } else if ( !_gender.equals( other._gender ) ) {
            return false;
        }
        if ( _name == null ) {
            if ( other._name != null ) {
                return false;
            }
        } else if ( !_name.equals( other._name ) ) {
            return false;
        }
        if ( _props == null ) {
            if ( other._props != null ) {
                return false;
            }
        } else if ( !_props.equals( other._props ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Person [_age=" + _age + ", _friends.size=" + _friends.size() + ", _gender=" + _gender + ", _name=" + _name
                + ", _props=" + _props + "]";
    }

}
