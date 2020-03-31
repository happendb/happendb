package logtime

import (
	"fmt"
	"regexp"
	"runtime"
	"time"

	"github.com/rs/zerolog/log"
)

// Elapsedf ...
func Elapsedf(what string, args ...interface{}) func() {
	t := time.Now()

	return func() {
		// Skip this function, and fetch the PC and file for its parent.
		pc, _, _, _ := runtime.Caller(1)

		// Retrieve a function object this functions parent.
		funcObj := runtime.FuncForPC(pc)

		// Regex to extract just the function name (and not the module path).
		runtimeFunc := regexp.MustCompile(`^.*\.(.*)$`)
		_ = runtimeFunc.ReplaceAllString(funcObj.Name(), "$1")

		log.Info().Msgf("[%v] took %v", fmt.Sprintf(what, args...), time.Since(t))
	}
}
