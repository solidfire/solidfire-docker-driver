package sfcli

import (
	"github.com/codegangsta/cli"
	"github.com/solidfire/solidfire-docker-driver/daemon"
)

var (
	daemonCmd = cli.Command{
		Name:  "daemon",
		Usage: "daemon related commands",
		Subcommands: []cli.Command{
			daemonStartCmd,
		},
	}

	daemonStartCmd = cli.Command{
		Name:  "start",
		Usage: "Start the SolidFire Docker Daemon: `start [options] NAME`",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "verbose, v",
				Usage: "Enable verbose/debug logging: `[--verbose]`",
			},
			cli.StringFlag{
				Name:  "config, c",
				Usage: "Config file for daemon (default: /var/lib/solidfire/solidfire.json): `[--config /var/lib/solidfire/solidfire.json]`",
			},
		},
		Action: cmdDaemonStart,
	}
)

func cmdDaemonStart(c *cli.Context) (err error) {
	verbose := c.Bool("verbose")
	cfg := c.String("config")
	if cfg == "" {
		cfg = "/var/lib/solidfire/solidfire.json"
	}
	daemon.Start(cfg, verbose)
	return err
}
