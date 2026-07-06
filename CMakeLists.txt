cmake_minimum_required(VERSION 3.16)

project(dperf LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_path(FASTRTPS_INCLUDE_DIR fastdds/dds/domain/DomainParticipantFactory.hpp)
find_library(FASTRTPS_LIBRARY fastrtps)
find_library(FASTCDR_LIBRARY fastcdr)

if(NOT FASTRTPS_INCLUDE_DIR OR NOT FASTRTPS_LIBRARY OR NOT FASTCDR_LIBRARY)
    message(FATAL_ERROR "Fast DDS/Fast CDR headers or libraries were not found")
endif()

add_library(perf_message_type
    generated/PerfMessage.cxx
    generated/PerfMessagePubSubTypes.cxx
)
target_include_directories(perf_message_type PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/generated
    ${FASTRTPS_INCLUDE_DIR}
)
target_link_libraries(perf_message_type PUBLIC
    ${FASTRTPS_LIBRARY}
    ${FASTCDR_LIBRARY}
)

add_executable(dperf_publisher src/publisher.cpp)
target_link_libraries(dperf_publisher PRIVATE perf_message_type)

add_executable(dperf_echo src/echo.cpp)
target_link_libraries(dperf_echo PRIVATE perf_message_type)
