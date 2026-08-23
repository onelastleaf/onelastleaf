cmake_minimum_required(VERSION 3.24)
project(__OLL_PLUGIN_NAME__ VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)
FetchContent_Declare(
  onelastleaf_plugin_sdk
  GIT_REPOSITORY https://github.com/onelastleaf/cpp-plugin-sdk.git
  GIT_TAG v0.1.0
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(onelastleaf_plugin_sdk)

add_executable(__OLL_PLUGIN_NAME__ src/main.cpp src/echo.cpp)
target_compile_features(__OLL_PLUGIN_NAME__ PRIVATE cxx_std_20)
target_link_libraries(__OLL_PLUGIN_NAME__ PRIVATE onelastleaf::plugin_sdk)
install(TARGETS __OLL_PLUGIN_NAME__ RUNTIME DESTINATION bin)

option(BUILD_TESTING "Build generated plugin tests" OFF)
if(BUILD_TESTING)
  enable_testing()
  add_executable(echo_test tests/echo_test.cpp src/echo.cpp)
  target_include_directories(echo_test PRIVATE src)
  add_test(NAME echo_test COMMAND echo_test)
endif()
