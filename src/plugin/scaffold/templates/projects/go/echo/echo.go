package echo

import (
	"strings"

	plugin "github.com/onelastleaf/go-plugin-sdk"
)

func Run(_ plugin.ActionContext, arguments []string) (plugin.ActionResult, error) {
	return plugin.StringResult(strings.Join(arguments, " ")), nil
}
