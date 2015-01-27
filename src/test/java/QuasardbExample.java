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

package com.b14.qdb;

import com.b14.qdb.*; 

class QuasardbExample {
    
    public static void main(String argv[]) {
        // Object needed to access your database
        Quasardb cache = null;
        
        // You need to configure your qdb instance thanks to a QuasardbConfig object.
        // It will tell the client API where the daemon is
        QuasardbConfig config = new QuasardbConfig();
        QuasardbNode node = new QuasardbNode("127.0.0.1", 2836);
        config.addNode(node);

        // the key has to be a string
        String keyName = "myKey";
        // note that the value can be any Java object
        String keyValue = "myValue";  
                  
        try {
            // Try to connect to your Quasardb instance
            System.out.println("Creating qdb instance...");
            cache = new Quasardb(config);
            
            if (cache != null) {
                // Adding some object
                System.out.println("Adding (" + keyName + ", " + keyValue + ")");
                cache.put(keyName, keyValue);
                
                // Check if a value has been stored at key
                System.out.println("Getting value for key " + keyName + "...");
                String value = cache.get(keyName);
                System.out.println("Result: " + value);

                // Removing your object
                System.out.println("Removing entry for " + keyName);
                cache.remove(keyName);               
            }
        } catch (Exception e) {
            System.err.println("Quasardb error for [" + keyName + "] ->" + e.getMessage());
            e.printStackTrace();
        } finally {
            // Closing your connection
            try {
                if (cache != null) {
                    cache.close();
                }
            } catch (QuasardbException e) {
                System.err.println("Quasardb error: " + e.getMessage());
            }
        } 
    }
}
