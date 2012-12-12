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
import java.util.Collection;
import java.util.Iterator;

@SuppressWarnings("serial")
public class Customer implements java.io.Serializable {

    //access methods for cmp fields
    private String id;
    private String firstName;
    private String lastName;
    private Collection<Address> addresses;
    private Collection<Subscription> subscriptions;
    
    public Customer() {
    }
    
    public String getCustomerID() {      //primary key
        return id;
    }
    public void setCustomerID(String id) {
        this.id=id;
    }
    
    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName=firstName;
    }

    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName=lastName;
    }

    public Customer(String id, String firstName, String lastName) {          
        setCustomerID(id);
        setFirstName(firstName);
        setLastName(lastName);
    }

    public Collection<Address> getAddresses(){
        return addresses;
    }
    public void setAddresses (Collection<Address> addresses){
        this.addresses=addresses;
    }

    public Collection<Subscription> getSubscriptions(){
        return subscriptions;
    }
    public void setSubscriptions (Collection<Subscription> subscriptions){
        this.subscriptions=subscriptions;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ArrayList getAddressList() {
        ArrayList list = new ArrayList();
        Iterator c = getAddresses().iterator();
        while (c.hasNext()) {
            list.add((Address)c.next());
        }
        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ArrayList getSubscriptionList() {
        ArrayList list = new ArrayList();
        Iterator c = getSubscriptions().iterator();
        while (c.hasNext()) {
            list.add((Subscription)c.next());
        }
        return list;
    }

    public void postCreate (){
        System.out.println("Customer::postCreate:");
    }

    public void ejbRemove() {
        System.out.println("Customer::postRemove");
    }
    
    private boolean flatEquals(final Collection<?> c1, final Collection<?> c2) {
        return c1 == c2 || c1 != null && c2 != null && c1.size() == c2.size();
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
        final Customer other = (Customer) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (addresses == null) {
            if (other.addresses != null) {
                return false;
            }
        } else if (!flatEquals(addresses, other.addresses)) {
            return false;
        }
        if (subscriptions == null) {
            if (other.subscriptions != null) {
                return false;
            }
        } else if (!flatEquals(subscriptions, other.subscriptions)) {
            return false;
        }
        if (firstName == null) {
            if (other.firstName != null) {
                return false;
            }
        } else if (!firstName.equals(other.firstName)) {
            return false;
        }
        if (lastName == null) {
            if (other.lastName != null) {
                return false;
            }
        } else if (!lastName.equals(other.lastName)) {
            return false;
        }
        return true;
    }
}
