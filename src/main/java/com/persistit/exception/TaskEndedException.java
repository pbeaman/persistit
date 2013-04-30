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

import com.persistit.Task;

/**
 * Thrown when a {@link Task} performing within the context of a task controller
 * is stopped.
 * 
 * @version 1.0
 */
public class TaskEndedException extends RuntimeException {
    private static final long serialVersionUID = 6439247775639387615L;

    public TaskEndedException() {
        super();
    }

    public TaskEndedException(final String msg) {
        super(msg);
    }
}
