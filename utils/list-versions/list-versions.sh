#!/bin/bash

# refname:strip=2: default tag name when format is not set
# creatordate is always defined for all tags
git tag --list 'v*-lts' 'v*-stable' --format='%(refname:short)	%(creatordate:short)' | sort -rV
