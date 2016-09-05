package sfcli

import (
	"fmt"

	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/alecthomas/units"
	"github.com/codegangsta/cli"
	"github.com/solidfire/solidfire-docker-driver/sfapi"
	"strconv"
)

var (
	snapshotCmd = cli.Command{
		Name:  "snapshot",
		Usage: "snapshot related commands",
		Subcommands: []cli.Command{
			snapshotCreateCmd,
			snapshotDeleteCmd,
			snapshotListCmd,
			snapshotRollbackCmd,
		},
	}

	snapshotCreateCmd = cli.Command{
		Name:  "create",
		Usage: "create a new snapshot: `create [options] SRC_VOLID`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "name",
				Usage: "Name to assign to snapshot, default is a time/date stamp: `[--name <SNAPSHOT_NAME>]`",
			},
		},
		Action: cmdSnapshotCreate,
	}

	snapshotListCmd = cli.Command{
		Name:  "list",
		Usage: "List existing snapshots: `list`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "volume",
				Usage: "Retrieve snapshots only for the specified volume ID: `[--volume VOLUME_ID]`",
			},
		},
		Action: cmdSnapshotList,
	}

	snapshotDeleteCmd = cli.Command{
		Name:   "delete",
		Usage:  "Delete an existing snapshot: `delete SNAPSHOT_ID`",
		Action: cmdSnapshotDelete,
	}

	snapshotRollbackCmd = cli.Command{
		Name:   "rollback",
		Usage:  "Rollback a volume to a previously taken snapshot: `rollback VOLUME_ID SNAPSHOT_ID`",
		Action: cmdSnapshotRollback,
	}
)

func cmdSnapshotCreate(c *cli.Context) (err error) {
	vID, _ := strconv.ParseInt(c.Args().First(), 10, 64)
	var req sfapi.CreateSnapshotRequest
	var result sfapi.CreateSnapshotResult

	req.VolumeID = vID
	if c.String("name") != "" {
		req.Name = c.String("name")
	}
	response, err := client.Request("CreateSnapshot", req, sfapi.NewReqID())
	if err != nil {
		log.Errorf("Create snapshot failed: ", err)
		return err
	}
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		log.Fatal(err)
	}
	s, err := client.GetSnapshot(result.Result.SnapshotID, "")

	fmt.Println("-------------------------------------------")
	fmt.Println("Succesfully Created Snapshot:")
	fmt.Println("-------------------------------------------")
	fmt.Println("ID:         ", s.SnapshotID)
	fmt.Println("Name:       ", s.Name)
	fmt.Println("VolumeID:   ", s.VolumeID)
	fmt.Println("Size (GiB):  ", s.TotalSize/int64(units.GiB))
	fmt.Println("-------------------------------------------")
	return err
}
func cmdSnapshotDelete(c *cli.Context) (err error) {
	for _, arg := range c.Args() {
		sID, _ := strconv.ParseInt(arg, 10, 64)
		client.DeleteSnapshot(sID)
	}
	return err
}

func cmdSnapshotRollback(c *cli.Context) (err error) {
	if len(c.Args()) < 2 {
		fmt.Println("Missing argument to rollback, requires <volumeID> <snapshotID>")
		return err
	}
	vID, _ := strconv.ParseInt(c.Args().First(), 10, 64)
	sID, _ := strconv.ParseInt(c.Args()[1], 10, 64)
	var req sfapi.RollbackToSnapshotRequest
	req.VolumeID = vID
	req.SnapshotID = sID
	client.RollbackToSnapshot(&req)
	return err
}

func cmdSnapshotList(c *cli.Context) (err error) {
	volID, _ := strconv.ParseInt(c.String("volume"), 10, 64)
	var req sfapi.ListSnapshotsRequest
	req.VolumeID = volID
	snapshots, err := client.ListSnapshots(&req)

	if err != nil {
		fmt.Println(err)
	} else {
		printSnapList(snapshots)
	}
	return err
}
