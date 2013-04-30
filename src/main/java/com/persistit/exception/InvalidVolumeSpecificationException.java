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
 * Thrown by {@link com.persistit.Volume} on an attempt to create a Volume with
 * invalid buffer size, initial size, extension size or maximum size..
 * 
 * @version 1.0
 */
public class InvalidVolumeSpecificationException extends IllegalArgumentException {
    private static final long serialVersionUID = 5310678046457279454L;

    public InvalidVolumeSpecificationException() {
        super();
    }

    public InvalidVolumeSpecificationException(final String msg) {
        super(msg);
    }

}
