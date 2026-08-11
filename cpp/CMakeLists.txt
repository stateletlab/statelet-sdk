cmake_minimum_required(VERSION 3.20)
project(statelet-sdk VERSION 0.1.3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Find dependencies ────────────────────────────────────────────────────────
# gRPC's package config pulls Protobuf in through find_dependency, i.e. in
# CONFIG mode. Asking for Protobuf in module mode as well leaves that export set
# half-defined and the configure step fails with "Some (but not all) targets in
# this export set were already defined" — so both are requested in CONFIG mode,
# gRPC first.
find_package(gRPC REQUIRED CONFIG)

# Protobuf is requested in CONFIG mode first so it resolves to the same targets
# gRPC's own config already pulled in — that is what Homebrew and vcpkg ship,
# and asking for module mode there leaves the export set half-defined ("Some
# (but not all) targets in this export set were already defined").
#
# Debian/Ubuntu's libprotobuf-dev ships no protobuf-config.cmake at all, only
# CMake's bundled FindProtobuf module, so a hard CONFIG requirement fails to
# configure there. Fall back instead of hard-failing; both modes define the
# protobuf::libprotobuf and protobuf::protoc targets used below.
find_package(Protobuf QUIET CONFIG)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED MODULE)
endif()

find_package(Threads REQUIRED)

# ── Generate protobuf / gRPC stubs ──────────────────────────────────────────
# Vendored copy, kept in sync by ../scripts/sync-proto.sh, so the directory
# stays buildable on its own (FetchContent / add_subdirectory / vcpkg port).
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/statelet.proto")
set(PROTO_OUT  "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_OUT})

set(PROTO_SRCS "${PROTO_OUT}/statelet.pb.cc")
set(PROTO_HDRS "${PROTO_OUT}/statelet.pb.h")
set(GRPC_SRCS  "${PROTO_OUT}/statelet.grpc.pb.cc")
set(GRPC_HDRS  "${PROTO_OUT}/statelet.grpc.pb.h")

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protobuf::protoc
    ARGS --grpc_out=${PROTO_OUT}
         --cpp_out=${PROTO_OUT}
         -I "${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf/gRPC C++ stubs"
)

# ── Library ──────────────────────────────────────────────────────────────────
add_library(statelet_sdk
    src/client.cpp
    src/agent_client.cpp
    src/agent_types.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_include_directories(statelet_sdk PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROTO_OUT}>
)

target_link_libraries(statelet_sdk PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

# ── Example ──────────────────────────────────────────────────────────────────
add_executable(statelet_example examples/example.cpp)
target_link_libraries(statelet_example PRIVATE statelet_sdk)

# ── Tests ────────────────────────────────────────────────────────────────────
# The value types and the step-JSON decoder carry no gRPC dependency, so this
# target builds without the generated stubs and runs without a server:
#   cmake --build . --target statelet_agent_types_test && ./statelet_agent_types_test
enable_testing()
add_executable(statelet_agent_types_test
    tests/agent_types_test.cpp
    src/agent_types.cpp
)
target_include_directories(statelet_agent_types_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
add_test(NAME agent_types COMMAND statelet_agent_types_test)
