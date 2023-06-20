cmake_minimum_required (VERSION 3.6)

project ("rucksack")

find_package(pubsub REQUIRED)

# Add source to this project's library
add_library(librucksack 
  "src/rucksack.cpp"
)
target_include_directories(librucksack PUBLIC include/ ${pubsub_INCLUDE_DIRS} ${pubsub_cpp_LIBRARIES})
target_link_libraries(librucksack ${pubsub_LIBRARIES})
set_property(TARGET librucksack PROPERTY CXX_STANDARD 11)
#set_target_properties(librucksack PROPERTIES DEBUG_POSTFIX "d")
#set_target_properties(librucksack PROPERTIES ARCHIVE_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_SOURCE_DIR}/out )
#set_target_properties(librucksack PROPERTIES ARCHIVE_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_CURRENT_SOURCE_DIR}/out )

# Build the command line tool for working with sacks
add_executable(rucksack "tools/rucksack.cpp")
target_include_directories(rucksack PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(rucksack librucksack ${pubsub_LIBRARIES} ${pubsub_cpp_LIBRARIES})
set_property(TARGET rucksack PROPERTY CXX_STANDARD 11)

# Build examples
add_executable(read_example "examples/read.cpp")
target_include_directories(read_example PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(read_example librucksack ${pubsub_LIBRARIES} ${pubsub_cpp_LIBRARIES})
add_dependencies(read_example librucksack)
set_property(TARGET read_example PROPERTY CXX_STANDARD 11)

add_executable(write_example "examples/write.cpp")
target_include_directories(write_example PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(write_example librucksack ${pubsub_LIBRARIES} ${pubsub_cpp_LIBRARIES})
add_dependencies(write_example librucksack)
set_property(TARGET write_example PROPERTY CXX_STANDARD 11)

function(my_export_target _target _include_dir)
    file(
        WRITE "${CMAKE_CURRENT_BINARY_DIR}/${_target}Config.cmake"
        "
            include(\"\$\{CMAKE_CURRENT_LIST_DIR\}/${_target}Targets.cmake\")
            set_property(
                TARGET ${_target}
                APPEND PROPERTY
                    INTERFACE_INCLUDE_DIRECTORIES \"${_include_dir}\"
            )
            set(${_target}_INCLUDE_DIRS \"${_include_dir}\")
            if(UNIX)
            set(${_target}_LIBRARIES \"${CMAKE_CURRENT_BINARY_DIR}/lib${_target}.a\")
            set(${_target}_DEBUG_LIBRARIES \"${CMAKE_CURRENT_BINARY_DIR}/lib${_target}.a\")
            else()
            set(${_target}_LIBRARIES \"${CMAKE_CURRENT_BINARY_DIR}/../../${_target}.lib\")
            set(${_target}_DEBUG_LIBRARIES \"${CMAKE_CURRENT_BINARY_DIR}/../../${_target}d.lib\")
            endif()
        "
    )

    export(TARGETS ${_target} FILE "${CMAKE_CURRENT_BINARY_DIR}/${_target}Targets.cmake")

    # NOTE: The following call can pollute your PC's CMake package registry
    #       See comments/alternatives below
    export(PACKAGE ${_target})
endfunction(my_export_target)

my_export_target(librucksack "${CMAKE_CURRENT_SOURCE_DIR}/include")
