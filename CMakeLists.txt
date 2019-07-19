CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp11.cmake)
INCLUDE (cmake/subdirs.cmake)
INCLUDE (cmake/cotire.cmake)
OPTION(BUILD_BENCHMARK "Build benchmark" OFF)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX11()

    IF (UNIX)
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "-O2 -Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "-O2 -pthread -Wall -Wextra -Werror -stdlib=libc++")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "-lpthread -stdlib=libc++")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib)
    INCLUDE_DIRECTORIES(include)

    SUBDIRS (
        clickhouse
        contrib/cityhash
        contrib/gtest
        contrib/lz4
        tests/simple
        ut
    )

    IF (BUILD_BENCHMARK)
        SUBDIRS(bench)
    ENDIF (BUILD_BENCHMARK)
