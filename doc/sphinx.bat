#!/bin/bash

sphinx-apidoc -f -o . ../src
make html
