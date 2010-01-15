#!/usr/bin/perl

# Copyright (C) 2010 Trustees of the University of Pennsylvania
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use File::Spec::Functions;
use File::Copy::Recursive qw(fcopy rcopy dircopy fmove rmove dirmove);
use File::Path;
use File::Copy;
use strict;

$| = 1;

if ($#ARGV < 0) {
    print "Must specify repository directory\n";
    exit;
}

my $repository = $ARGV[0];
my %jars = (log4j => catfile('log4j', 'log4j', '1.2.15', 'log4j-1.2.15.jar'),
	    bdb => catfile('com', 'sleepycat', 'je', '3.3.69', 'je-3.3.69.jar'),
	    db2 => catfile('com', 'ibm', 'db2', 'db2jcc', '9.1.2', 'db2jcc-9.1.2.jar'),
	    db2lic => catfile('com', 'ibm', 'db2', 'db2jcc-license', '9.1.2', 'db2jcc-license-9.1.2.jar')
	);
my @packages = qw{edu.upenn.cis.data edu.upenn.cis.orchestra.datamodel edu.upenn.cis.orchestra.util edu.upenn.cis.orchestra.optimization edu.upenn.cis.orchestra.p2pqp edu.upenn.cis.orchestra.predicate com.experlog.zql edu.upenn.cis.orchestra.repository.model.beans};
my @otherFiles = qw{tpch.properties};
my $base = 'engine/target/classes/';

my $target = 'testHarness';

print "Deleting old build\n";

rmtree($target, 0, 1);
mkdir $target;
chdir $target;

my ($jarname, $jar);

while (($jarname, $jar) = each %jars) {
    print "Extracting $jarname\n";
    my $file = catfile($repository, $jar);
    system('jar', 'xf', $file);
    rmtree('META-INF', 0, 0);
}

my $package;
for $package (@packages) {
    print "Copying $package\n";
    $package =~ s/\./\//g;
    rcopy(catfile('..', $base, $package), $package);
}

my ($file, $inFile);
for $file (@otherFiles) {
	print "Copying $file\n";
	$inFile = catfile('..', $base, $file);
	copy($inFile, $file) || die "Copy failed: $!";
}

chdir '..';

open(MANIFEST,">manifest");
print MANIFEST "Main-Class: edu.upenn.cis.data.TestHarness\n";
close(MANIFEST);

unlink 'testHarness/edu/upenn/cis/data/local.properties';

unlink 'testHarness.jar';

print "Building executable jar file\n";

system 'jar', 'cfm', 'testHarness.jar', 'manifest', '-C', catdir('testHarness'), '.';
unlink 'manifest';
