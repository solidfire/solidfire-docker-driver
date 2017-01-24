package daemon

import (
	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
	"path/filepath"
)

var (
	defaultDir = filepath.Join(volume.DefaultDockerRootDirectory, "solidfire")
)

func Start(cfgFile string, debug bool) {
	if debug == true {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	d := New(cfgFile)
	h := volume.NewHandler(d)
	log.Info(h.ServeUnix("solidfire", 0))
}
