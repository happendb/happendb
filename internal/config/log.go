package config

import (
	"flag"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogging ...
func InitLogging() {
	debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	log.Logger = log.
		Output(
			zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: time.RFC3339Nano,
			}).
		With().
		Caller().
		Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}
