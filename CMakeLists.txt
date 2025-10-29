cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME onelake)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES 
    src/onelake_extension.cpp
    src/onelake_api.cpp
    src/onelake_credentials.cpp
    src/onelake_secret.cpp
    src/onelake_catalog.cpp
    src/storage/onelake_storage_extension.cpp
    src/storage/onelake_catalog_set.cpp
    src/storage/onelake_schema_set.cpp
    src/storage/onelake_schema_entry.cpp
    src/storage/onelake_table_set.cpp
    src/storage/onelake_table_entry.cpp
    src/storage/onelake_transaction_manager.cpp
    src/storage/onelake_transaction.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL, CURL and JsonCpp in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
