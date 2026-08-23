#include "echo.hpp"

std::string echo(const std::vector<std::string>& arguments) {
  std::string output;
  for (const auto& value : arguments) {
    if (!output.empty()) output += ' ';
    output += value;
  }
  return output;
}
