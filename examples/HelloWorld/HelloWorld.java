/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

