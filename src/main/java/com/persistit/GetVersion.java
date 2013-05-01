/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Access the build version string from a resource included in the persistit jar
 * file. The static {@link #main} method writes the version string out to
 * System.out.
 * 
 * @author peter
 * 
 */
public class GetVersion {

    private final String _version;

    /**
     * Write the build version string to {@link System#out}
     * 
     * @param a
     * @throws Exception
     */
    public static void main(final String[] a) throws Exception {
        System.out.println(getVersionString());
    }

    /**
     * @return the build version string
     */
    public static String getVersionString() {
        try {
            return new GetVersion("persistit_version").toString();
        } catch (final IOException e) {
            return "UNKNOWN: " + e;
        }
    }

    private GetVersion(final String jarResource) throws IOException {
        InputStream in = null;
        BufferedReader reader = null;
        try {
            in = GetVersion.class.getResourceAsStream(jarResource);
            reader = new BufferedReader(new InputStreamReader(in));
            _version = reader.readLine();
        } catch (final IOException e) {
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
