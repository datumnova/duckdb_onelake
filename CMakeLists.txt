cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME onelake)

set(CURL_LIBRARY "-lcurl")
set(CURL_INCLUDE_DIR "/usr/include/curl")

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
find_package(azure-identity-cpp CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES_COMMON
    src/onelake_extension.cpp
    src/onelake_api.cpp
    src/onelake_credentials.cpp
    src/onelake_secret.cpp
    src/onelake_catalog.cpp
  src/onelake_delta_writer.cpp
    src/storage/onelake_storage_extension.cpp
    src/storage/onelake_catalog_set.cpp
    src/storage/onelake_http_util.cpp
    src/storage/onelake_schema_set.cpp
    src/storage/onelake_schema_entry.cpp
    src/storage/onelake_table_set.cpp
    src/storage/onelake_table_entry.cpp
    src/storage/onelake_path_util.cpp
    src/storage/onelake_insert.cpp
    src/storage/onelake_delete.cpp
    src/storage/onelake_update.cpp
    src/storage/onelake_transaction_manager.cpp
    src/storage/onelake_transaction.cpp
)

# Parser extension only for loadable extension to avoid symbol conflicts
set(EXTENSION_SOURCES_LOADABLE
    ${EXTENSION_SOURCES_COMMON}
    src/onelake_parser_extension.cpp
)

# Build the Rust-based Delta writer unconditionally so both static and loadable
# extensions can link against it.
set(RUST_CRATE_DIR ${CMAKE_CURRENT_LIST_DIR}/rust/onelake_delta_writer)
set(RUST_BUILD_DIR ${CMAKE_BINARY_DIR}/onelake_delta_writer)
set(RUST_HEADER ${RUST_BUILD_DIR}/include/onelake_delta_writer.h)
set(RUST_TARGET_TRIPLE "")

if(APPLE)
  set(REQUESTED_OSX_ARCH "")
  if(CMAKE_OSX_ARCHITECTURES)
    list(LENGTH CMAKE_OSX_ARCHITECTURES OSX_ARCH_COUNT)
    if(OSX_ARCH_COUNT GREATER 1)
      message(FATAL_ERROR "Building the OneLake extension for multiple macOS architectures simultaneously is not supported. Requested architectures: ${CMAKE_OSX_ARCHITECTURES}")
    endif()
    list(GET CMAKE_OSX_ARCHITECTURES 0 REQUESTED_OSX_ARCH)
  endif()

  if(NOT REQUESTED_OSX_ARCH)
    set(REQUESTED_OSX_ARCH ${CMAKE_SYSTEM_PROCESSOR})
  endif()

  string(TOLOWER "${REQUESTED_OSX_ARCH}" REQUESTED_OSX_ARCH_LOWER)
  if(REQUESTED_OSX_ARCH_LOWER STREQUAL "x86_64" OR REQUESTED_OSX_ARCH_LOWER STREQUAL "amd64")
    set(RUST_TARGET_TRIPLE "x86_64-apple-darwin")
  elseif(REQUESTED_OSX_ARCH_LOWER STREQUAL "arm64" OR REQUESTED_OSX_ARCH_LOWER STREQUAL "aarch64")
    set(RUST_TARGET_TRIPLE "aarch64-apple-darwin")
  else()
    message(FATAL_ERROR "Unsupported macOS architecture '${REQUESTED_OSX_ARCH}'.")
  endif()
elseif(WIN32)
  set(RUST_TARGET_TRIPLE "x86_64-pc-windows-msvc")
endif()

if(WIN32)
  set(RUST_LIB_FILENAME onelake_delta_writer.lib)
else()
  set(RUST_LIB_FILENAME libonelake_delta_writer.a)
endif()

if(RUST_TARGET_TRIPLE)
  set(RUST_TARGET_RELEASE_DIR ${RUST_CRATE_DIR}/target/${RUST_TARGET_TRIPLE}/release)
else()
  set(RUST_TARGET_RELEASE_DIR ${RUST_CRATE_DIR}/target/release)
endif()

set(RUST_LIB_SOURCE ${RUST_TARGET_RELEASE_DIR}/${RUST_LIB_FILENAME})
set(RUST_LIB ${RUST_BUILD_DIR}/lib/${RUST_LIB_FILENAME})

set(CARGO_BUILD_ARGS --manifest-path ${RUST_CRATE_DIR}/Cargo.toml --release)
if(RUST_TARGET_TRIPLE)
  list(APPEND CARGO_BUILD_ARGS --target ${RUST_TARGET_TRIPLE})
endif()

set(RUST_TARGET_PREBUILD_COMMAND)
if(RUST_TARGET_TRIPLE)
  set(RUST_TARGET_PREBUILD_COMMAND COMMAND rustup target add ${RUST_TARGET_TRIPLE})
endif()

set(RUST_ENV_VARS ONELAKE_HEADER_DIR=${RUST_BUILD_DIR}/include)
if(APPLE)
  if(CMAKE_OSX_DEPLOYMENT_TARGET)
    set(RUST_MACOS_DEPLOYMENT_TARGET ${CMAKE_OSX_DEPLOYMENT_TARGET})
  else()
    set(RUST_MACOS_DEPLOYMENT_TARGET "11.0")
  endif()
  list(APPEND RUST_ENV_VARS MACOSX_DEPLOYMENT_TARGET=${RUST_MACOS_DEPLOYMENT_TARGET})
endif()

add_custom_command(
  OUTPUT ${RUST_LIB} ${RUST_HEADER}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${RUST_BUILD_DIR}/lib
  COMMAND ${CMAKE_COMMAND} -E make_directory ${RUST_BUILD_DIR}/include
  ${RUST_TARGET_PREBUILD_COMMAND}
  COMMAND ${CMAKE_COMMAND} -E env ${RUST_ENV_VARS} cargo build ${CARGO_BUILD_ARGS}
  COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_SOURCE} ${RUST_LIB}
  WORKING_DIRECTORY ${RUST_CRATE_DIR}
  DEPENDS ${RUST_CRATE_DIR}/src/lib.rs ${RUST_CRATE_DIR}/Cargo.toml ${RUST_CRATE_DIR}/build.rs
  VERBATIM
)

add_custom_target(onelake_delta_writer ALL
  DEPENDS ${RUST_LIB} ${RUST_HEADER}
)

build_static_extension(${TARGET_NAME} "${EXTENSION_SOURCES_COMMON}")
build_loadable_extension(${TARGET_NAME} " " "${EXTENSION_SOURCES_LOADABLE}")

set_property(TARGET ${EXTENSION_NAME} PROPERTY CXX_STANDARD 17)
set_property(TARGET ${EXTENSION_NAME} PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} PROPERTY CXX_STANDARD 17)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} PROPERTY CXX_STANDARD_REQUIRED ON)

find_package(BZip2 REQUIRED)

# Static Lib
add_dependencies(${EXTENSION_NAME} onelake_delta_writer)
add_dependencies(${LOADABLE_EXTENSION_NAME} onelake_delta_writer)

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity BZip2::BZip2 ${RUST_LIB})
target_include_directories(
  ${EXTENSION_NAME}
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity ${RUST_BUILD_DIR}/include)

# Loadable extension
target_link_libraries(
  ${TARGET_NAME}_loadable_extension OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity BZip2::BZip2 ${RUST_LIB})
target_include_directories(
  ${TARGET_NAME}_loadable_extension
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl JsonCpp::JsonCpp Azure::azure-identity ${RUST_BUILD_DIR}/include)

if(WIN32)
  target_link_libraries(${EXTENSION_NAME} Userenv Ntdll)
  target_link_libraries(${TARGET_NAME}_loadable_extension Userenv Ntdll)
endif()

# Allow duplicate constexpr static symbols from catalog entry headers
if(NOT APPLE AND NOT WIN32)
  target_link_options(${TARGET_NAME}_loadable_extension PRIVATE "LINKER:--allow-multiple-definition")
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
