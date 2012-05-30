#/bin/sh
#
# Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3 (only) of the
# License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# 
# This program may also be available under different license terms. For more
# information, see www.akiban.com or contact licensing@akiban.com.
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
rm -rf ../../target/sphinx/source
mkdir -p ../../target/sphinx/source
mkdir -p ../../target/sphinx/classes
mkdir -p ../../target/sphinx/html

cp ../index.rst ../../target/sphinx/source
cp ../conf.py ../../target/sphinx/source

javac -d ../../target/sphinx/classes -cp ../../target/classes/ src/*.java

java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../ReleaseNotes.rst out=../../target/sphinx/source/ReleaseNotes.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../BasicAPI.rst out=../../target/sphinx/source/BasicAPI.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Configuration.rst out=../../target/sphinx/source/Configuration.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../GettingStarted.rst out=../../target/sphinx/source/GettingStarted.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Management.rst out=../../target/sphinx/source/Management.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Miscellaneous.rst out=../../target/sphinx/source/Miscellaneous.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../PhysicalStorage.rst out=../../target/sphinx/source/PhysicalStorage.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Security.rst out=../../target/sphinx/source/Security.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Serialization.rst out=../../target/sphinx/source/Serialization.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html
java -cp ../../target/sphinx/classes:../../target/classes SphinxDocPrep in=../Transactions.rst out=../../target/sphinx/source/Transactions.rst base=http://www.akiban.com/ak-docs/admin/persistit/apidocs index=../../target/site/apidocs/index-all.html

sphinx-build -a  ../../target/sphinx/source ../../target/sphinx/html
