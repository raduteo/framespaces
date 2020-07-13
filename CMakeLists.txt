cmake_minimum_required(VERSION 3.15)
project(framespaces)
set(CMAKE_CXX_STANDARD 20)
message("Build type " ${CMAKE_BUILD_TYPE})
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        message("Adding coverage flags")
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping -ftemplate-backtrace-limit=0 -DDEBUG)
        add_link_options(-fprofile-instr-generate)
    else()
        message("Not Adding coverage flags")
    endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-fconcepts)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        message("Adding coverage flags gcc ")
        add_compile_options(-fprofile-generate -ftemplate-backtrace-limit=0 -DDEBUG)
        add_link_options(-fprofile-instr-generate)
    else()
        message("Not Adding coverage flags")
    endif()
endif()

get_cmake_property(vars CACHE_VARIABLES)
foreach(var ${vars})
    get_property(currentHelpString CACHE "${var}" PROPERTY HELPSTRING)
    if("${currentHelpString}" MATCHES "No help, variable specified on the command line." OR "${currentHelpString}" STREQUAL "")
        # message("${var} = [${${var}}]  --  ${currentHelpString}") # uncomment to see the variables being processed
        list(APPEND CL_ARGS "-D${var}=${${var}}")
    endif()
endforeach()

include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)
ExternalProject_Add(arrow
        PREFIX external/arrow
        GIT_REPOSITORY "https://github.com/apache/arrow.git"
        GIT_TAG master
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        CMAKE_ARGS ${CL_ARGS} -DARROW_COMPUTE=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        SOURCE_SUBDIR cpp)
message("CL_ARGS" ${CL_ARGS})
set(ARROW_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow/cpp/src")
set(ARROW_INCLUDE_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow-build/src")
include_directories(${ARROW_INCLUDE_DIR} ${ARROW_INCLUDE_GEN_DIR})
SET(ARROW_LIB_DIR ${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow-build/debug)
add_library(arrow_shared SHARED IMPORTED)
message("ARROW_LIB_DIR" ${ARROW_LIB_DIR} )
set_target_properties(arrow_shared PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow.dylib)
add_library(arrow_testing SHARED IMPORTED)
set_target_properties(arrow_testing PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow_testing.dylib)


add_library(ForexConnect SHARED IMPORTED)


add_subdirectory(test)
add_subdirectory(benchmark)
add_subdirectory(spaces)
add_subdirectory(3rdparty/google/googletest)
add_subdirectory(3rdparty/google/benchmark)



add_executable(fragmentation_demo main.cpp spaces/Compact.cpp FixedSizeAllocator.h  Leaf.h)
add_dependencies(fragmentation_demo arrow)
target_link_libraries(fragmentation_demo arrow_shared)
add_dependencies(framespaces_tst arrow)
add_dependencies(framespaces_compact_tst arrow)
