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
 * Thrown by decoding methods of the {@link com.persistit.Value} class when the
 * serialized byte array is corrupt. This is a catastrophic failure that
 * signifies external volume file corruption.
 * 
 * @version 1.0
 */
public class MalformedValueException extends RuntimeException {
    private static final long serialVersionUID = 5868710861424952291L;

    public MalformedValueException() {
        super();
    }

    public MalformedValueException(final String msg) {
        super(msg);
    }
}
