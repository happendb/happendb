package logtime

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// Elapsedf ...
func Elapsedf(what string, args ...interface{}) func() {
	t := time.Now()

	return func() {
		log.Info().Msgf("[%v] took %v", fmt.Sprintf(what, args...), time.Since(t))
	}
}
