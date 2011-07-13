#/bin/sh
#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# ---------------------
#
# Builds the Akiban Persistit doc set.  Currently this process is based on
# the asciidoc tool (http://www.methods.co.nz/asciidoc/).
#
# Here are the steps:
# 1. Run a Java program AsciiDocPrep to prepare a text asciidoc file.
#    Among other things, AsciiDocPrep fills in JavaDoc hyperlinks.
# 2. Run asciidoc to generate an html file.
# 3. Use sed to replace some characters.  Turns out asciidoc doesn't like
#    to link to URLs having spaces, so AsciDocPrep replaces those spaces
#    with the "`" character.  This step converts those back to spaces.
#
# Run this script from the root of the persistit source directory. This
# script writes changes only into a directory /tmp/akiban-persistit-doc.
# The end-product files, user_guide.html and user_guide.xml are written
# there.
#
rm -rf /tmp/akiban-persistit-doc
mkdir /tmp/akiban-persistit-doc
javac -d /tmp/akiban-persistit-doc -cp ../../core/target/classes/ src/*.java
java -cp /tmp/akiban-persistit-doc:../../core/target/classes AsciiDocPrep in=../TOC.txt out=/tmp/akiban-persistit-doc/doc.txt base=apidocs index=../../core/target/site/apidocs/index-all.html
asciidoc -a toc -n -d book -b xhtml11 -o /tmp/akiban-persistit-doc/doc.html /tmp/akiban-persistit-doc/doc.txt
asciidoc -a toc -n -d book -b docbook -o /tmp/akiban-persistit-doc/doc.xml /tmp/akiban-persistit-doc/doc.txt
sed s/\`/\ / /tmp/akiban-persistit-doc/doc.html > /tmp/akiban-persistit-doc/user_guide.html
sed s/\`/\ / /tmp/akiban-persistit-doc/doc.xml > /tmp/akiban-persistit-doc/user_guide.xml

