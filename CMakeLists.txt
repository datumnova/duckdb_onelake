cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME onelake)

set(CURL_LIBRARY "-lcurl")
set(CURL_INCLUDE_DIR "/usr/include/curl")


option(ONELAKE_ENABLE_AZURE "Enable Azure SDK integration (azure-core-cpp)" ON)

if(MINGW)
  message(STATUS "Detected MinGW — disabling Azure SDK integration to avoid vcpkg build failures on CI")
  set(ONELAKE_ENABLE_AZURE OFF CACHE BOOL "Disable Azure SDK on MinGW" FORCE)
endif()

# Hint CMake to the bundled vcpkg installation when the toolchain isn't providing jsoncpp.
find_package(jsoncpp CONFIG QUIET)

if(TARGET JsonCpp::JsonCpp OR TARGET jsoncpp)
  if(TARGET JsonCpp::JsonCpp)
    set(JSONCPP_TARGET JsonCpp::JsonCpp)
  else()
    set(JSONCPP_TARGET jsoncpp)
  endif()
else()
  include(FetchContent)
  FetchContent_Declare(
    jsoncpp
    GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
    GIT_TAG 1.9.5
  )
  FetchContent_MakeAvailable(jsoncpp)
  if(TARGET JsonCpp::JsonCpp)
    set(JSONCPP_TARGET JsonCpp::JsonCpp)
  elseif(TARGET jsoncpp)
    set(JSONCPP_TARGET jsoncpp)
  else()
    message(FATAL_ERROR "jsoncpp not available")
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

if(ONELAKE_ENABLE_AZURE)
  # prefer package config via vcpkg when available
  find_package(azure-core CONFIG QUIET)

  # If not found, optionally fetch the SDK as a fallback (FetchContent) or fail early depending on your preference
  if(NOT TARGET Azure::azure_core AND NOT TARGET azure::core)
    include(FetchContent)
    message(STATUS "azure-core not found via find_package; fetching azure-sdk-for-cpp via FetchContent")
    FetchContent_Declare(
      azure_sdk_for_cpp
      GIT_REPOSITORY https://github.com/Azure/azure-sdk-for-cpp.git
      GIT_TAG v1.16.1
    )
    FetchContent_MakeAvailable(azure_sdk_for_cpp)
  endif()

  if(TARGET Azure::azure_core OR TARGET azure::core)
    target_link_libraries(${EXTENSION_NAME} PRIVATE Azure::azure_core) 
  else()
    message(FATAL_ERROR "Azure SDK requested but unavailable")
  endif()
else()
  message(STATUS "Azure SDK integration disabled")
endif()

# Link OpenSSL, CURL and JsonCpp in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl ${JSONCPP_TARGET} Azure::azure-identity)
target_include_directories(
  ${EXTENSION_NAME}
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl ${JSONCPP_TARGET} Azure::azure-identity)

target_link_libraries(
  ${TARGET_NAME}_loadable_extension OpenSSL::SSL OpenSSL::Crypto CURL::libcurl ${JSONCPP_TARGET} Azure::azure-identity)
target_include_directories(
  ${TARGET_NAME}_loadable_extension
  PRIVATE OpenSSL::SSL OpenSSL::Crypto CURL::libcurl ${JSONCPP_TARGET} Azure::azure-identity)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
