#!/bin/sh
#
# Shell script to extract relevant fields from a PubMed Central file list, which is
# downloadable from: http://www.ncbi.nlm.nih.gov/pmc/tools/ftp/
# The extracted fields are used to map downloaded PMC files to their PMC IDs.
#   Author: Tom Hicks.
#   Last Modified: Initial documentation and checkin.
#
cut -f1,3 - | tail -n +2 | sed -e's/.tar.gz//g' | sed -e's;../../;;' | sort -bf
