/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Access the build version string from a resource included in the persistit jar file.
 * The static {@link #main} method writes the version string out to System.out.
 * @author peter
 *
 */
public class GetVersion {

    private final String _version;

    /**
     * Write the build version string to {@link System#out}
     * @param a
     * @throws Exception
     */
    public static void main(String[] a) throws Exception {
        System.out.println(getVersionString());
    }

    /**
     * @return the build version string
     */
    public static String getVersionString() {
        try {
            return new GetVersion("persistit_version").toString();
        } catch (IOException e) {
            return "UNKNOWN: " + e;
        }
    }

    private GetVersion(String jarResource) throws IOException {
        InputStream in = null;
        BufferedReader reader = null;
        try {
            in = GetVersion.class.getResourceAsStream(jarResource);
            reader = new BufferedReader(new InputStreamReader(in));
            _version = reader.readLine();
        } catch (IOException e) {
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (in != null) {
                in.close();
            }
        }
    }

    @Override
    public String toString() {
        return _version.toString();
    }
}
