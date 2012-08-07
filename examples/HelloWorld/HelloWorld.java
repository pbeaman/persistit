/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        Persistit db = new Persistit();
        try {
            // Read configuration from persistit.properties, allocates
            // buffers, opens Volume(s), and performs recovery processing
            // if necessary.
            //
            db.initialize();
            //
            // Create an Exchange, which is a thread-private facade for
            // accessing data in a Persistit Tree. This Exchange will
            // access a Tree called "greetings" in a Volume called
            // "hwdemo". It will create a new Tree by that name
            // if one does not already exist.
            //
            Exchange dbex = db.getExchange("hwdemo", "greetings", true);
            //
            // Set up the Value field of the Exchange.
            //
            dbex.getValue().put("World");
            //
            // Set up the Key field of the Exchange.
            //
            dbex.getKey().append("Hello");
            //
            // Ask Persistit to store this key/value pair into the Tree.
            //
            dbex.store();
            //
            // Prepare to traverse all the keys in the Tree (of which there
            // is currently only one!) and for each key display its value.
            //
            dbex.getKey().to(Key.BEFORE);
            while (dbex.next()) {
                System.out.println(dbex.getKey().reset().decode() 
                        + " " + dbex.getValue().get());
            }
            db.releaseExchange(dbex);
        } finally {
            // Always close Persistit. If the application does not do
            // this, Persistit's background threads will keep the JVM from
            // terminating.
            //
            db.close();
        }
    }
}

