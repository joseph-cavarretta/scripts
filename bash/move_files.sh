#! /bin/bash

# Moves files from specific sub-folders to a new destination

_source='/path/to/source/'
_dest='/path/to/dest/'

# list sub folders to move files from
declare -a paths=(
    [0]="subfolder-1/"
    [1]="subfolder-2/"
    [2]="subfolder-3/"
    [3]="subfolder-4/"
)