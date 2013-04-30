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
 * Thrown if the journal files are corrupt. Generally it will be necessary to
 * delete the journal to resolve this. In so doing, critical information needed
 * to recover the state of one or more {@link com.persistit.Volume}s may be
 * lost.
 * 
 * @version 1.0
 */
public class CorruptJournalException extends PersistitIOException {
    private static final long serialVersionUID = -5397911019132612370L;

    public CorruptJournalException(final String msg) {
        super(msg);
    }
}
