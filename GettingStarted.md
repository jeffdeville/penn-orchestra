# Setting Up Your Development Environment in Eclipse #

Here's the way that we set up our development environment:

  1. Get the Eclipse Classic package from, for example, http://www.eclipse.org/downloads/
  1. Install the [M2Eclipse](http://m2eclipse.sonatype.org/) Eclipse plugin. At a minimum you'll need the following items:
    * Maven Integration/Maven Embedder
    * Maven Integration/Maven Integration for Eclipse
    * Maven Optional Components/Maven SCM Handler for Subclipse
  1. Install the [Subclipse](http://subclipse.tigris.org/) Eclipse plugin. At a minimum you'll need the required items:
    * Subclipse/Subclipse
    * Subclipse/Subversion Client Adapter
    * Subclipse/Subversion JavaHL Native Library Adapter
  1. Install the [TestNG](http://testng.org/doc/eclipse.html) Eclipse plugin
  1. Fire up Eclipse and switch over to the _SVN Repository Exploring_ perspective and create a repository at http://penn-orchestra.googlecode.com/svn/ if you're not a commiter or https://penn-orchestra.googlecode.com/svn/ if you're a committer
  1. Navigate to http(s)://penn-orchestra.googlecode.com/svn/branches, expand it, and right-click (ctrl-click on Macs) on orchestra-0.1.x and choose _Check out as Maven Project_
    * Some of us like to choose the Name Template `[`artifactId`]`-`[`version`]` under _Advanced_ to give a quick indicator of which branch we're working on but that doesn't seem to work on OS X
    * Hit _Finish_
  1. Switch over to the Java perspective

# Running the TestNG tests #

All the automated tests are controlled by testng.xml files. We currently have five of these files:

  * #### `engine/testng.xml` ####
> > These are the unit tests which do not rely on a DB2 instance. It can be run by the Eclipse TestNG plugin right out of the box. It is the only one of the four test suites which is run by the default Maven build.

  * #### `gui/testng.xml` ####
> > The will contain simple tests of the GUI, and non-GUI tests which rely on the update store script built by the engine module.

The remaining testng.xml files contain the tests which rely on a DB2 instance. The default Maven build does not run these tests. To have Maven run these tests the profile _requires-database-tests_ must be active. The database connection information is supplied to the tests via Maven resource filtering, so certain variables, listed below, need to be set as described [here](http://maven.apache.org/plugins/maven-resources-plugin/examples/filter.html).

  * #### `engine/requires-database-testng.xml` ####
> > This contains the unit tests which rely on a DB2 instance. The variables which need to be set are:
      * `testng.db-url`
      * `testng.db-user`
      * `testng.db-password`

  * #### `engine/orchestra-test-testng.xml` ####
> > This runs the functional tests. The variables which need to be set are:
      * `testng.db-url.pPODPeer1`
      * `testng.db-url.pPODPeer2`
      * `testng.db-user`
      * `testng.db-password`

  * #### `gui/gui-orchestra-test-testng.xml` ####
> > This runs the functional test which uses the GUI. The variables which need to be set are:
      * `testng.db-url.pPODPeer1`
      * `testng.db-url.pPODPeer2`
      * `testng.db-user`
      * `testng.db-password`