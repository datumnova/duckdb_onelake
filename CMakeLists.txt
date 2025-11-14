cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME onelake)

set(CURL_LIBRARY "-lcurl")
set(CURL_INCLUDE_DIR "/usr/include/curl")
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_BUILD_TYPE release)

# Configure Azure SDK to use curl instead of WinHTTP
set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DBUILD_TRANSPORT_WINHTTP=OFF" "-DBUILD_TRANSPORT_CURL=ON")

# Hint CMake to the bundled vcpkg installation when the toolchain isn't providing jsoncpp.
if(NOT DEFINED jsoncpp_DIR)
  set(_jsoncpp_guess "${CMAKE_CURRENT_LIST_DIR}/vcpkg_installed/x64-linux/share/jsoncpp")
  if(EXISTS "${_jsoncpp_guess}/jsoncppConfig.cmake")
    set(jsoncpp_DIR "${_jsoncpp_guess}" CACHE PATH "JsonCpp config directory")
  endif()
endif()

project(${TARGET_NAME})
include_directories(src/include)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(jsoncpp REQUIRED)
find_package(azure-identity-cpp CONFIG)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES 
    src/onelake_extension.cpp
    src/onelake_api.cpp
    src/onelake_credentials.cpp
    src/onelake_secret.cpp
    src/onelake_catalog.cpp
    src/onelake_parser_extension.cpp
    src/storage/onelake_storage_extension.cpp
    src/storage/onelake_catalog_set.cpp
    src/storage/onelake_http_util.cpp
    src/storage/onelake_schema_set.cpp
    src/storage/onelake_schema_entry.cpp
    src/storage/onelake_table_set.cpp
    src/storage/onelake_table_entry.cpp
    src/storage/onelake_transaction_manager.cpp
    src/storage/onelake_transaction.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set_property(TARGET ${EXTENSION_NAME} PROPERTY CXX_STANDARD 17)
set_property(TARGET ${EXTENSION_NAME} PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} PROPERTY CXX_STANDARD 17)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} PROPERTY CXX_STANDARD_REQUIRED ON)

# Link OpenSSL, CURL and JsonCpp in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity)
target_include_directories(
  ${EXTENSION_NAME}
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity)

target_link_libraries(
  ${TARGET_NAME}_loadable_extension OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity)
target_include_directories(
  ${TARGET_NAME}_loadable_extension
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
