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

# A script to read in uniprot_sprot.dat from SwissProt and put
# selected fields of interest into files.

use strict;

my %func;

open(ENTRIES, ">entries.txt");

my $os = "";
my $id;
my %refs;

while (<>) {
	if (/^ID   ([A-Z0-9]{1,5}_[A-Z0-9]{1,5})/) {
		$id = $1;
	} elsif (/^ID  /) {
	} elsif (/^OS   (.+)\n$/) {
		$os .= $1;
	} elsif (/^CC   -!- FUNCTION: (.+)\./) {
		++$func{$1};
	} elsif (/^DR   ([^;]+); ([^;]+)/) {
		$refs{$1} = $2;
	} elsif (/^\/\/$/) {
		chop($os);
		$os = substr($os,0,120);
		print ENTRIES $id, "\t", $os;
		foreach (sort keys(%refs)) {
			print ENTRIES "\t", $_, "\t", substr($refs{$_}, 0, 50);
		}
		print ENTRIES "\n";
		$os = "";
		undef %refs;
	}
}

my @ids;
my @org;
my @func;

my $key;

foreach $key (keys(%func)) {
	push(@func, {val => $key, count => $func{$key}});
}

open(FUNC,">func.txt");

my $rec;

foreach $rec (sort byCount @func) {
	printf FUNC "%s\t%s\n", $rec->{'val'}, $rec->{'count'};
}

close(FUNC);
close(ENTRIES);

sub byCount {
	return $b->{'count'} <=> $a->{'count'};
}
