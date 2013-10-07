#!/bin/bash

rm -rf full stripped
python -c "f = open('mixtapes.counter', 'r+');f.seek(0);f.write(str(1+int(f.read())));f.close"
