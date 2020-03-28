package time

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Elapsedf ...
func Elapsedf(what string, args ...interface{}) func() {
	start := time.Now()

	return func() {
		log.Debugf("%s took %v\n", fmt.Sprintf(what, args...), time.Since(start))
	}
}
