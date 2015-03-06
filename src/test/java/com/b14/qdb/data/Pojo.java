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

/**
 * Pojo class representing a user.<br>
 * <br>
 * This class contains {@link Pojo} reference in order to test store/retrieve functions in quasardb.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 */
public class Pojo {
    private String text = "something";
    private String nullField;
    private Pojo child;
    private Pojo child2;
    private float abc = 1.2f;
    private int optional;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getNullField() {
        return nullField;
    }

    public void setNullField(String nullField) {
        this.nullField = nullField;
    }

    public Pojo getChild() {
        return child;
    }

    public void setChild(Pojo child) {
        this.child = child;
    }

    public Pojo getChild2() {
        return child2;
    }

    public void setChild2(Pojo child2) {
        this.child2 = child2;
    }

    public float getAbc() {
        return abc;
    }

    public void setAbc(float abc) {
        this.abc = abc;
    }

    public int getOptional() {
        return optional;
    }

    public void setOptional(int optional) {
        this.optional = optional;
    }

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
        Pojo other = (Pojo) obj;
        if (Float.floatToIntBits(abc) != Float.floatToIntBits(other.abc)) {
            return false;
        }
        if (child == null) {
            if (other.child != null) {
                return false;
            }
        } else if (child != this && !child.equals(other.child)) {
            return false;
        }
        if (child2 == null) {
            if (other.child2 != null) {
                return false;
            }
        } else if (child2 != this && !child2.equals(other.child2)) {
            return false;
        }
        if (nullField == null) {
            if (other.nullField != null) {
                return false;
            }
        } else if (!nullField.equals(other.nullField)) {
            return false;
        }
        if (text == null) {
            if (other.text != null) {
                return false;
            }
        } else if (!text.equals(other.text)) {
            return false;
        }
        return true;
    }
}
