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

