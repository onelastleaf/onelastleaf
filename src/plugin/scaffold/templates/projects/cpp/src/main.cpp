#include "echo.hpp"

#include <onelastleaf/plugin_sdk.hpp>

int main() {
  onelastleaf::Plugin plugin{"__OLL_PLUGIN_ID__", "0.1.0"};
  plugin.action("echo", "Return the supplied arguments", [](auto&, const auto& arguments) {
    return onelastleaf::ActionResult::string(echo(arguments));
  });
  return plugin.run();
}
