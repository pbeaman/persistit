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

package com.persistit.exception;

/**
 * Thrown when an application attempts to perform operations on a
 * {@link com.persistit.Volume} that has been closed.
 * 
 * @version 1.0
 */
public class VolumeClosedException extends PersistitException {
    private static final long serialVersionUID = -1577835912746168786L;

    public VolumeClosedException() {
        super();
    }

    public VolumeClosedException(final String msg) {
        super(msg);
    }
}
