/*
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.Iterator;
import java.util.Map;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.PersistitMap;

public class PersistitMapDemo
{
    public static void main(String[] args)
    throws Exception 
    {
        // This program uses PersistitMap, an implementation of 
        // java.util.SortedMap to store a copy of the system properties.
        // Each time you run this program it will compare the current
        // system properties to those that were stored previously and will
        // display any differences.
        //
        // PersistitMap works just like any other Map except that its values
        // are persistent.  (See the API documentation for PersistitMap for
        // a few other considerations.)
        //
    	Persistit persistit = new Persistit();
        try
        {
            persistit.initialize();
            Exchange dbex = new Exchange(persistit, "pmdemo", "properties", true);
            //
            // Create a PersistitMap over this exchange.  The map will be
            // non-empty if this program has already been run previously.
            //
            PersistitMap persistitMap = new PersistitMap(dbex);
            //
            // All Persistit databases below are invoked through the Map
            // interface.
            //
            if (persistitMap.size() == 0)
            {
                System.out.println("This is the first time PersistitMapDemo has run");
            }
            else
            {
                System.out.println("Comparing property values:");
                for (Iterator iter = System.getProperties().entrySet().iterator();
                     iter.hasNext();)
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    String name = (String)entry.getKey();
                    String newValue = (String)entry.getValue();
                    
                    String oldValue = (String)persistitMap.remove(name);
                    if (oldValue == null)
                    {
                        System.out.println(
                            "New value for " + name + " is '" + newValue + "'");
                    }
                    else if (!newValue.equals(oldValue))
                    {
                        System.out.println(
                            "Value changed for " + name +
                            " from '" + oldValue + "' to '" + newValue + "'");
                    }
                }
            }

            for (Iterator iter = persistitMap.entrySet().iterator();
                 iter.hasNext();)
            {
                Map.Entry entry = (Map.Entry)iter.next();
                String name = (String)entry.getKey();
                String oldValue = (String)entry.getValue();
                
                System.out.println(
                    "Old value for " + name + "='" + oldValue + "' is gone");
            }

            persistitMap.putAll(System.getProperties());
        }
        finally
        {
            // Always close Persistit. If the application does not do 
            // this, Persistit's background threads will keep the JVM from
            // terminating.
            //
            persistit.close();
        }
    }
}