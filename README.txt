# /* <GENERIC-HEADER - BEGIN>
#  *
#  * $(COMPANY) $(COPYRIGHT)
#  *
#  * Created on: Nov, 20, 2009
#  * Created by: Thomas Hazel
#  *
#  * </GENERIC-HEADER - END> */

#
# Temporary install (until akiba has its own repo )
#
$ mvn install:install-file -DgroupId=javax.help -DartifactId=search -Dversion=2.0.02 -Dpackaging=jar -Dfile=jhbasic.jar

#
# Install jars in your local repository
#
$ mvn source:jar install

