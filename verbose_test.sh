#!/bin/bash

env PYTHONHASHSEED=random PYTHONPATH=. nosetests -v --with-yanc --with-xtraceback --nocapture $@