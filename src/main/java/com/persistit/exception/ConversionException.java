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
 * Thrown by encoding and decoding methods of the {@link com.persistit.Value}
 * and {@link com.persistit.Key} classes when conversion to or from an value's
 * serialized form cannot be completed.
 * 
 * @version 1.0
 */
public class ConversionException extends RuntimeException {
    private static final long serialVersionUID = -5255687227174752145L;

    public ConversionException() {
        super();
    }

    public ConversionException(final String msg) {
        super(msg);
    }

    public ConversionException(final Throwable t) {
        super(t);
    }

    public ConversionException(final String msg, final Throwable t) {
        super(msg, t);
    }

}
