#include "echo.hpp"
#include <cassert>

int main() {
  assert(echo({"one", "two"}) == "one two");
}
