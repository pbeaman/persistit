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
 * The superclass for all checked Persistit&trade; Exceptions. This class can
 * also serve as a wrapper for other Exception types.
 * 
 * @version 1.0
 */
public class PersistitException extends Exception {

    private static final long serialVersionUID = -2971539608220570084L;

    protected PersistitException() {
        super();
    }

    protected PersistitException(final String msg) {
        super(msg);
    }

    public PersistitException(final Throwable t) {
        super(t);
    }

    public PersistitException(final String msg, final Throwable t) {
        super(msg, t);
    }
}
