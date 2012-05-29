/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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

