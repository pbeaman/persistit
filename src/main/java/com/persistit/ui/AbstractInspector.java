/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.ui;

import javax.swing.JPanel;

/**
 * A JPanel that drops into the UI to allow inspection of Key and Value values.
 * Inspectors offer multiple views, e.g., as a displayable string, a hex dump,
 * the toString() method of a reconstituted object, and via the structure of an
 * object discovered through reflection.
 * 
 * @author Peter Beaman
 * @version 1.0
 */
abstract class AbstractInspector extends JPanel {
    protected AdminUI _adminUI;
    protected InspectorPanel _host;

    protected void setup(final AdminUI ui, final InspectorPanel host) {
        _adminUI = ui;
        _host = host;
    }

    protected void waiting() {
    }

    protected void refreshed() {
    }

    protected void nullData() {
    }
}
