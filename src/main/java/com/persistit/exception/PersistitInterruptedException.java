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

package com.persistit.exception;

/**
 * This is a wrapper for an {@link InterruptedException}. If a Persistit thread
 * is interrupted when sleeping or waiting, it throws an instance of this
 * Exception. This allows the caller of a Persistit method to catch
 * {@link PersistitException}s without also needing to catch
 * InterruptedExceptions.
 * <p />
 * Before throwing a PersistitInterruptedException Persistit reasserts the
 * <code>interrupted</code> status of the current Thread so that a subsequent
 * call to sleep or wait will once again be interrupted.
 * 
 * @version 1.0
 */
public class PersistitInterruptedException extends PersistitException {
    private static final long serialVersionUID = 9108205596568958490L;

    public PersistitInterruptedException(final InterruptedException ioe) {
        super(ioe);
        Thread.currentThread().interrupt();
    }

}
