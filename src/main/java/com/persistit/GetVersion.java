/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
