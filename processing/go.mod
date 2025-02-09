module github.com/kaspa-live/kaspa-graph-inspector/processing

go 1.16

require (
	github.com/go-pg/pg/extra/pgdebug v0.2.0
	github.com/go-pg/pg/v10 v10.6.2
	github.com/golang-migrate/migrate/v4 v4.14.1
	github.com/jessevdk/go-flags v1.4.0
	github.com/kaspanet/kaspad v0.11.13
	github.com/pkg/errors v0.9.1
)

replace github.com/kaspanet/kaspad => ../../../kaspanet/kaspad
