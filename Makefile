PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=onelake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Default to the repo-local vcpkg toolchain so manifest dependencies resolve.
export VCPKG_TOOLCHAIN_PATH ?= ${PROJ_DIR}vcpkg/scripts/buildsystems/vcpkg.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Ensure vcpkg is bootstrapped before running clang-tidy so manifest dependencies can be restored in CI.
tidy-check: vcpkg-bootstrap

.PHONY: vcpkg-bootstrap
vcpkg-bootstrap:
	@if [ ! -x "${PROJ_DIR}vcpkg/vcpkg" ]; then \
		echo "Bootstrapping vcpkg..."; \
		"${PROJ_DIR}vcpkg/bootstrap-vcpkg.sh" -disableMetrics; \
	fi