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
package com.b14.qdb.hadoop.mahout;

import java.util.Date;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * A simple {@link Preference} encapsulating an item, a preference value and a timestamp.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see Preference
 * @version master
 * @since 1.3.0
 */
public class QuasardbPreference implements Preference {
    private final long userID;
    private final long itemID;
    private float preference;
    private long createAt;

    /**
     * Build a {@link Preference} object by providing user id, item id, preference.
     * Default creation will be created.
     * 
     * @param userID user identification
     * @param itemID item identification
     * @param preference preference score
     */
    public QuasardbPreference(long userID, long itemID, float preference) {
        this(userID, itemID, preference, (new Date()).getTime());
    }
    
    /**
     * Build a {@link Preference} object by providing user id, item id, preference and creation date
     * 
     * @param userID user identification
     * @param itemID item identification
     * @param preference preference score
     * @param createAt preference's creation date
     * @since 1.3.0
     */
    public QuasardbPreference(long userID, long itemID, float preference, long createAt) {
        this.userID = userID;
        this.itemID = itemID;
        if (Float.isNaN(preference)) {
            throw new IllegalArgumentException("NaN value provided => " + preference);
        }
        this.preference = preference;
        this.createAt = createAt;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public long getUserID() {
        return this.userID;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public long getItemID() {
        return this.itemID;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public float getValue() {
        return this.preference;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void setValue(float value) {
        if (Float.isNaN(value)) {
            throw new IllegalArgumentException("NaN value provided => " + value);
        }
        this.preference = value;
    }

    /**
     * Retrieve creation date for current preference.
     * 
     * @return preference's creation date in long format.
     */
    public long getCreateAt() {
        return this.createAt;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     * @since 1.3.0
     */
    @Override
    public String toString() {
        return "QuasardbPreference [userID=" + userID + ", itemID=" + itemID + ", preference=" + preference + ", createAt=" + createAt + "]";
    }

    /**
     * {@inheritDoc}
     *  
     * @see java.lang.Object#hashCode()
     * @since 1.3.0
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (createAt ^ (createAt >>> 32));
        result = prime * result + (int) (itemID ^ (itemID >>> 32));
        result = prime * result + Float.floatToIntBits(preference);
        result = prime * result + (int) (userID ^ (userID >>> 32));
        return result;
    }

    /**
     * {@inheritDoc}
     *  
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 1.3.0
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
        QuasardbPreference other = (QuasardbPreference) obj;
        if (createAt != other.createAt) {
            return false;
        }
        if (itemID != other.itemID) {
            return false;
        }
        if (Float.floatToIntBits(preference) != Float.floatToIntBits(other.preference)) {
            return false;
        }
        if (userID != other.userID) {
            return false;
        }
        return true;
    }
}
