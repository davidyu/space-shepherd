.PHONY: configure update static

configure:
	virtualenv flask

update:
	pushd fe/lib; sh update.sh; popd

static: update
	mkdir -p static/vendor
	cp -f fe/lib/*.js static/vendor/
