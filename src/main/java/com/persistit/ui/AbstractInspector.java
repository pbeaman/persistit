/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Mar 21, 2005
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

    protected void setup(AdminUI ui, InspectorPanel host) {
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
