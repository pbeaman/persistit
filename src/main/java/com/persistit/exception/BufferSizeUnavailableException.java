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
 * Thrown if there is no {@link com.persistit.BufferPool} with buffers matching
 * the page size of a {@link com.persistit.Volume} being opened.
 * 
 * @version 1.0
 */
public class BufferSizeUnavailableException extends PersistitException {
    private static final long serialVersionUID = 7231838587511494996L;

    public BufferSizeUnavailableException(final String msg) {
        super(msg);
    }

}
