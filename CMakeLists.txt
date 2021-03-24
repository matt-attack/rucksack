# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.6)

project ("rucksack")

# Add source to this project's executable.
add_library(librucksack 
  "src/rucksack.cpp"
)
target_include_directories(librucksack PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(librucksack pubsub ${pubsub_LIBRARIES})
set_property(TARGET librucksack PROPERTY CXX_STANDARD 11)

add_executable(rucksack "tools/rucksack.cpp")
target_include_directories(rucksack PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(rucksack pubsub_cpp librucksack ${pubsub_LIBRARIES})
set_property(TARGET rucksack PROPERTY CXX_STANDARD 11)

# Build examples
add_executable(read_example "examples/read.cpp")
target_include_directories(read_example PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(read_example pubsub_cpp librucksack ${pubsub_LIBRARIES})
set_property(TARGET read_example PROPERTY CXX_STANDARD 11)

add_executable(write_example "examples/write.cpp")
target_include_directories(write_example PUBLIC include/ ${pubsub_INCLUDE_DIRS})
target_link_libraries(write_example pubsub_cpp librucksack ${pubsub_LIBRARIES})
set_property(TARGET write_example PROPERTY CXX_STANDARD 11)
