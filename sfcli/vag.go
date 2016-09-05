package sfcli

import (

	//"github.com/alecthomas/units"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/solidfire/solidfire-docker-driver/sfapi"
	"strconv"
)

var (
	vagCmd = cli.Command{
		Name:  "vag",
		Usage: "VAG (Volume Access Group) related commands",
		Subcommands: []cli.Command{
			vagCreateCmd,
			vagListCmd,
		},
	}

	vagCreateCmd = cli.Command{
		Name:  "create",
		Usage: "create a new Volume Access Group: `create [options] NAME`",
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "initiator",
				Usage: "Initiator IQN(s) to add to the newly create VAG: `[--initiator <IQN-1> --initiator <IQN-2>...]`",
			},
			cli.StringSliceFlag{
				Name:  "volume",
				Usage: "Volume ID(s) to add to the newly create VAG: `[--volume <VOLID-1> --volume <VOLID-2>...]`",
			},
		},
		Action: cmdVagCreate,
	}

	vagListCmd = cli.Command{
		Name:   "list",
		Usage:  "List Volume Access Groups: `list`",
		Action: cmdVagList,
	}
)

func cmdVagList(c *cli.Context) (err error) {
	var req sfapi.ListVolumeAccessGroupsRequest
	groups, err := client.ListVolumeAccessGroups(&req)
	if err != nil {
		return err
	}
	fmt.Println(groups)
	return err
}

func cmdVagCreate(c *cli.Context) (err error) {
	var req sfapi.CreateVolumeAccessGroupRequest
	req.Name = c.Args().First()
	for _, init := range c.StringSlice("initiator") {
		req.Initiators = append(req.Initiators, init)
	}

	for _, vol := range c.StringSlice("volume") {
		id, _ := strconv.ParseInt(vol, 10, 64)
		req.Volumes = append(req.Volumes, id)
	}

	vagID, _ := client.CreateVolumeAccessGroup(&req)
	fmt.Println("VAG ID is: %s", vagID)
	return nil
}
