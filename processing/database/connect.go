package database

import (
	"fmt"
	"github.com/go-pg/pg/extra/pgdebug"
	"github.com/go-pg/pg/v10"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/database/block_hashes_to_ids"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/infrastructure/logging"
	"github.com/pkg/errors"
	"strings"
)

var (
	log              = logging.Logger()
	allowedTimeZones = map[string]struct{}{
		"UTC":     {},
		"Etc/UTC": {},
	}
)

// Connect connects to the database mentioned in the config variable.
func Connect(connectionString string) (*Database, error) {
	migrator, driver, err := openMigrator(connectionString)
	if err != nil {
		return nil, err
	}
	isCurrent, version, err := isCurrent(migrator, driver)
	if err != nil {
		return nil, errors.Wrapf(err, "error checking whether the database is current")
	}
	if !isCurrent {
		log.Warnf("Database is not current (version %d). Migrating...", version)
		err := migrate(connectionString)
		if err != nil {
			return nil, errors.Wrapf(err, "could not migrate database")
		}
	}

	connectionOptions, err := pg.ParseURL(connectionString)
	if err != nil {
		return nil, err
	}

	pgDB := pg.Connect(connectionOptions)
	pgDB.AddQueryHook(pgdebug.DebugHook{
		Verbose: false, // Set to `true` to print all queries
	})

	err = validateTimeZone(pgDB)
	if err != nil {
		return nil, errors.Wrapf(err, "could not validate database timezone")
	}

	return &Database{
		database:         pgDB,
		blockHashesToIDs: block_hashes_to_ids.New(),
	}, nil
}

func validateTimeZone(db *pg.DB) error {
	var timeZone string
	_, err := db.QueryOne(pg.Scan(&timeZone), `SELECT current_setting('TIMEZONE') as time_zone`)

	if err != nil {
		return errors.WithMessage(err, "some errors were encountered when "+
			"checking the database timezone:")
	}

	if _, ok := allowedTimeZones[timeZone]; !ok {
		return errors.Errorf("to prevent conversion errors - Kasparov should only run with "+
			"a database configured to use one of the allowed timezone. Currently configured timezone "+
			"is %s. Allowed time zones: %s", timeZone, allowedTimezonesString())
	}
	return nil
}

func allowedTimezonesString() string {
	keys := make([]string, 0, len(allowedTimeZones))
	for allowedTimeZone := range allowedTimeZones {
		keys = append(keys, fmt.Sprintf("'%s'", allowedTimeZone))
	}
	return strings.Join(keys, ", ")
}
