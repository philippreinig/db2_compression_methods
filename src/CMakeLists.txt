add_executable(main main.cpp)
target_link_libraries(main Catch2::Catch2WithMain cereal)
target_compile_options(main PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>: -Wall -Wextra -Wpedantic -Werror>
        )
target_compile_features(main PRIVATE cxx_std_17)
set_property(TARGET main PROPERTY CXX_STANDARD 17)

#catch_discover_tests(main)
add_test(main main)

add_subdirectory(core)
