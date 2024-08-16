package migrate

import (
	"go.uber.org/zap"
)

type Option []OptionFn
type OptionFn func(m *Migrator)

// WithClean clean database
func WithClean(scheme ...string) OptionFn {
	return func(m *Migrator) {
		m.cleanScheme = scheme
	}
}

// WithLogger implement logger
func WithLogger(logger *zap.Logger) OptionFn {
	return func(m *Migrator) {
		m.logger = logger
	}
}
