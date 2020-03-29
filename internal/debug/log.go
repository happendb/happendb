package debug

import (
	"fmt"
	"time"
)

// Elapsedf ...
func Elapsedf(what string, args ...interface{}) func() string {
	start := time.Now()

	return func() string {
		return fmt.Sprintf("%s took %v\n", fmt.Sprintf(what, args...), time.Since(start))
	}
}
