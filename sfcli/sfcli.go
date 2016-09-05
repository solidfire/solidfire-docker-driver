package sfcli

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/solidfire/solidfire-docker-driver/sfapi"
	"strings"
	"unicode/utf8"
)

var client, _ = sfapi.New()

// cmdNofFound routines borrowed from rackspace/rack
// https://github.com/rackspace/rack/blob/master/commandsuggest.go
func cmdNotFound(c *cli.Context, command string) {
	app := c.App
	var choices []string
	for _, cmd := range app.Commands {
		choices = append(choices, cmd.Name)
	}
	//choices := globalOptionsNames(app)
	currentMin := 50
	bestSuggestion := ""
	for _, choice := range choices {
		similarity := levenshtein(choice, command)
		tmpMin := min(currentMin, similarity)
		if tmpMin < currentMin {
			bestSuggestion = choice
			currentMin = tmpMin
		}
	}

	suggestion := []string{fmt.Sprintf("Unrecognized command: %s", command),
		"",
		"Did you mean this?",
		fmt.Sprintf("\t%s\n", bestSuggestion),
		"",
	}

	fmt.Fprintf(c.App.Writer, strings.Join(suggestion, "\n"))
}

func levenshtein(a, b string) int {
	f := make([]int, utf8.RuneCountInString(b)+1)

	for j := range f {
		f[j] = j
	}

	for _, ca := range a {
		j := 1
		fj1 := f[0] // fj1 is the value of f[j - 1] in last iteration
		f[0]++
		for _, cb := range b {
			mn := min(f[j]+1, f[j-1]+1) // delete & insert
			if cb != ca {
				mn = min(mn, fj1+1) // change
			} else {
				mn = min(mn, fj1) // matched
			}

			fj1, f[j] = f[j], mn // save f[j] to fj1(j is about to increase), update f[j] to mn
			j++
		}
	}

	return f[len(f)-1]
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func NewCli(version string) *cli.App {
	app := cli.NewApp()
	app.Name = "solidfire"
	app.Version = version
	app.Author = "John Griffith <john.griffith@solidfire.com>"
	app.Usage = "CLI for SolidFire clusters"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "loglevel",
			Value:  "info",
			Usage:  "Specifies the logging level (debug|warning|error)",
			EnvVar: "LogLevel",
		},
		cli.StringFlag{
			Name:   "svip, s",
			Value:  "",
			Usage:  "Specifies the SVIP of the SolidFire cluster including iscsi Port (\"1.1.1.1:3260\") .",
			EnvVar: "SVIP",
		},
		cli.StringFlag{
			Name:   "defaultAccountID, a",
			Value:  "",
			Usage:  "Specifies a default SolidFire AccountID to use for operations.",
			EnvVar: "ACCOUNTID",
		},
		cli.StringFlag{
			Name:  "endpoint, e",
			Value: "",
			Usage: "Specifies the endpoint of the SolidFire cluster to issue cmds to, " +
				"\n\t(\"https://admin:admin@172.16.140.21/json-rpc/7.0\") .",
			EnvVar: "ENDPOINT",
		},
		cli.StringFlag{
			Name:  "config, c",
			Value: "",
			Usage: "Specify SolidFire config file to use (overrides env variables if set).",
		},
	}
	app.CommandNotFound = cmdNotFound
	app.Before = initClient
	app.Commands = []cli.Command{
		volumeCmd,
		snapshotCmd,
		vagCmd,
		daemonCmd,
		//accountCmd,
	}
	return app
}

func initClient(c *cli.Context) error {
	cfgFile := c.GlobalString("config")
	if cfgFile != "" {
		client, _ = sfapi.NewFromConfig(cfgFile)
		conf, _ := sfapi.ProcessConfig(cfgFile)
		client.Config = &conf
	} else {
		client, _ = sfapi.New()
	}
	updateLogLevel(c)
	return nil
}

func updateLogLevel(c *cli.Context) (err error) {
	switch c.String("loglevel") {
	case "panic":
		log.SetLevel(log.PanicLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	}
	return err
}
