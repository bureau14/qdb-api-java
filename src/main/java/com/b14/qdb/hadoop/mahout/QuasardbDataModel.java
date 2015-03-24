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

import java.io.IOException;
import java.util.Collection;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbConfig;
import com.b14.qdb.QuasardbException;
import com.b14.qdb.QuasardbNode;
import com.google.common.collect.Lists;

/**
 * A {@link DataModel} backed by quasardb.<br>
 * <br> 
 * This class expects an already loaded quasardb instance with keys starting with prefix <b>recommender.user_</b> and containing {@link QuasardbPreference} values. 
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see DataModel
 * @see QuasardbPreference
 * @version master
 * @since 1.3.0
 */
public class QuasardbDataModel implements DataModel {
    private static final long serialVersionUID = -2902235729393836305L;
    private static final String DEFAULT_QDB_KEYSPACE = "recommender.user_";
    private static final String DEFAULT_QDB_HOST = "localhost";
    private static final int DEFAULT_QDB_PORT = 2836;
    
    private DataModel delegate;
    private transient QuasardbConfig qdbConfig = new QuasardbConfig();
    private transient Quasardb qdb = null;
    private String preferencesPrefix = null;
    
    public QuasardbDataModel() throws IOException {
        this(DEFAULT_QDB_HOST, DEFAULT_QDB_PORT, DEFAULT_QDB_KEYSPACE);
    }
    
    public QuasardbDataModel(String host, int port) throws IOException {
        this(host, port, DEFAULT_QDB_KEYSPACE);
    }
    
    public QuasardbDataModel(String host, int port, String preferencesPrefix) throws IOException {
        this.qdbConfig.addNode(new QuasardbNode(host, port));
        this.preferencesPrefix = preferencesPrefix;
        buildModel();
    }

    /**
     * Build a {@link GenericDataModel} based on all stored {@link QuasardbPreference} in provided quasardb instance.
     * 
     * @throws IOException when quasardb instance is unreachable.
     */
    private void buildModel() throws IOException {
        FastByIDMap<Collection<Preference>> userIDPrefMap = new FastByIDMap<Collection<Preference>>();
        qdb = new Quasardb(this.qdbConfig);
        try {
            qdb.connect();
            for (String key : qdb.startsWith(preferencesPrefix)) {
                QuasardbPreference qdbPref = qdb.get(key);
                long userID = qdbPref.getUserID();
                Collection<Preference> userPrefs = userIDPrefMap.get(userID);
                if (userPrefs == null) {
                    userPrefs = Lists.newArrayListWithCapacity(2);
                    userIDPrefMap.put(userID, userPrefs);
                }
                userPrefs.add(qdbPref);
            }
            delegate = new GenericDataModel(GenericDataModel.toDataMap(userIDPrefMap, true));
        } catch (QuasardbException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        try {
            buildModel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public LongPrimitiveIterator getUserIDs() throws TasteException {
        return delegate.getUserIDs();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public PreferenceArray getPreferencesFromUser(long userID) throws TasteException {
        return delegate.getPreferencesFromUser(userID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        return delegate.getItemIDsFromUser(userID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public LongPrimitiveIterator getItemIDs() throws TasteException {
        return delegate.getItemIDs();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
        return delegate.getPreferencesForItem(itemID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public Float getPreferenceValue(long userID, long itemID) throws TasteException {
        return delegate.getPreferenceValue(userID, itemID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public Long getPreferenceTime(long userID, long itemID) throws TasteException {
        return delegate.getPreferenceTime(userID, itemID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public int getNumItems() throws TasteException {
        return delegate.getNumItems();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public int getNumUsers() throws TasteException {
        return delegate.getNumUsers();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
        return delegate.getNumUsersWithPreferenceFor(itemID);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) throws TasteException {
        return delegate.getNumUsersWithPreferenceFor(itemID1, itemID2);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void setPreference(long userID, long itemID, float value) throws TasteException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void removePreference(long userID, long itemID) throws TasteException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public boolean hasPreferenceValues() {
        return delegate.hasPreferenceValues();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public float getMaxPreference() {
        return delegate.getMaxPreference();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public float getMinPreference() {
        return delegate.getMinPreference();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public String toString() {
      return "QuasardbDataModel";
    }
}
