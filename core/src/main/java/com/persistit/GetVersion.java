/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetVersion{

    public static void main (String[] a) throws Exception
    {
        System.out.println (new GetVersion("version/persistit_version") );
    }

    String version;

    public GetVersion(String jarResource) throws IOException {
        InputStream in = null;
        BufferedReader reader = null;
        try {
            in = ClassLoader.getSystemResourceAsStream( jarResource );
            reader = new BufferedReader ( new InputStreamReader ( in ) );
            version = reader.readLine();
        } catch(IOException e) {
            throw e;
        }
        finally {
            if (reader != null) {
                reader.close();
            }
            if (in != null) {
                in.close();
            }
        }
    }
    @Override
        public String toString(){
            return   version.toString();  
        }
}

