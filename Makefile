.PHONY: fmt test build tag

PLUGIN ?=

fmt:
	./scripts/fmt.sh

test:
	./scripts/check.sh

build:
	@test -n "$(PLUGIN)" || (echo "usage: make build PLUGIN=<name>" >&2; exit 1)
	./scripts/build-plugin.sh "$(PLUGIN)"

tag:
	./scripts/create-tag.sh
