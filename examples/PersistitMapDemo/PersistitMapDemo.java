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

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.PersistitMap;

public class PersistitMapDemo {
    
    public static void main(String[] args) throws Exception {
        // This program uses PersistitMap, an implementation of
        // java.util.SortedMap to store a copy of the system properties.
        // Each time you run this program it will compare the current
        // system properties to those that were stored previously and will
        // display any differences.
        //
        // PersistitMap works just like any other Map except that its values
        // are persistent. (See the API documentation for PersistitMap for
        // a few other considerations.)
        //
        Persistit persistit = new Persistit();
        persistit.initialize();
        
        try {
            Exchange dbex = persistit.getExchange("pmdemo", "properties", true);
            //
            // Create a PersistitMap over this exchange. The map will be
            // non-empty if this program has already been run previously.
            //
            PersistitMap<Object, Object> persistitMap = new PersistitMap<Object, Object>(dbex);
            //
            // All Persistit database operations below are invoked through the
            // Map interface.
            //
            if (persistitMap.size() == 0) {
                System.out.println("This is the first time PersistitMapDemo has run");
            } else {
                System.out.println("Comparing property values:");
                SortedMap<Object, Object> sorted = new TreeMap<Object, Object>(System.getProperties());

                for (Iterator<Map.Entry<Object, Object>> iter = sorted.entrySet().iterator(); iter.hasNext();) {
                    Map.Entry<Object, Object> entry = iter.next();
                    Object name = entry.getKey();
                    Object newValue = entry.getValue();
                    Object oldValue = persistitMap.remove(name);
                    if (oldValue == null) {
                        System.out.println("New value " + name + " is '" + newValue + "'");
                    } else if (!newValue.equals(oldValue)) {
                        System.out.println("Value changed " + name + " from '" + oldValue + "' to '" + newValue
                                + "'");
                    }
                }
            }

            for (Iterator<Map.Entry<Object, Object>> iter = persistitMap.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<Object, Object> entry = iter.next();
                Object name = entry.getKey();
                Object oldValue = entry.getValue();
                System.out.println("Old value " + name + "='" + oldValue + "' is gone");
                iter.remove();
            }

            persistitMap.putAll(System.getProperties());
            
        } finally {
            // Always close Persistit. If the application does not do
            // this, Persistit's background threads will keep the JVM from
            // terminating.
            //
            persistit.close();
        }
    }
}

