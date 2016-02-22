#!/bin/bash

# usage: download url target
# where target is the name of the local file target
download() {
    local url="$1"
    local target="$2"
    [ -f "$target" ] || curl -k --location "$url" -o "$target"
}

echo "updating vendor code..."

echo updating libraries...
download https://raw.githubusercontent.com/gka/chroma.js/master/chroma.js chroma.js
download https://raw.githubusercontent.com/mbostock/d3/master/d3.js d3.js
# download https://raw.githubusercontent.com/ccampbell/mousetrap/master/mousetrap.js mousetrap.js

echo updating typescript definition files...
download https://raw.githubusercontent.com/borisyankov/DefinitelyTyped/master/chroma-js/chroma-js.d.ts chroma-js.d.ts
download https://raw.githubusercontent.com/DefinitelyTyped/DefinitelyTyped/master/d3/d3.d.ts d3.d.ts

echo all done.
