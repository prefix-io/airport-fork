PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Core extensions that we need for testing
#CORE_EXTENSIONS='httpfs'

# Configuration of extension
EXT_NAME=airport
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile