package sfcli

import (
	"errors"
	"fmt"

	"github.com/alecthomas/units"
	"github.com/codegangsta/cli"
	"github.com/solidfire/solidfire-docker-driver/sfapi"
	"strconv"
	"strings"
)

var (
	volumeCmd = cli.Command{
		Name:  "volume",
		Usage: "volume related commands",
		Subcommands: []cli.Command{
			volumeCreateCmd,
			volumeCloneCmd,
			volumeDeleteCmd,
			volumeListCmd,
			volumeAttachCmd,
			volumeDetachCmd,
			volumeAddToVag,
			volumeRollbackCmd,
		},
	}

	volumeCreateCmd = cli.Command{
		Name:  "create",
		Usage: "create a new volume: `create [options] NAME`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "size",
				Usage: "size of volume in bytes or GiB: `[--size 1073741824|1GiB]`",
			},
			cli.StringFlag{
				Name:  "account",
				Usage: "account id to assign volume: `[--account 488]`",
			},
			cli.StringFlag{
				Name:  "qos",
				Usage: "min,max and burst qos settings for new volume: `[--qos 1000,5000,15000]`",
			},
			cli.StringFlag{
				Name:  "vag",
				Usage: "Volume Access Group to add volume to on create: `[--vag 8]`",
			},
			cli.StringFlag{
				Name:  "type",
				Usage: "Specify a volume type as defined in a SolidFire config file: `[--type Gold]`",
			},
		},
		Action: cmdVolumeCreate,
	}

	volumeCloneCmd = cli.Command{
		Name:   "clone",
		Usage:  "create a clone of an existing volume: `clone [options] EXISTING_VOLID NAME`",
		Action: cmdVolumeClone,
	}

	volumeRollbackCmd = cli.Command{
		Name:   "rollback",
		Usage:  "rollback a volume to a previously taken snapshot `rollback [options] VOLUME_ID SNAPSHOT_ID`",
		Action: cmdVolumeRollback,
	}

	volumeDeleteCmd = cli.Command{
		Name:  "delete",
		Usage: "delete an existing volume: `delete VOLUME-ID`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "range",
				Value: "",
				Usage: ": deletes a range of volume ID's `[--range <startID-endID>]`",
			},
		},
		Action: cmdVolumeDelete,
	}

	volumeListCmd = cli.Command{
		Name:  "list",
		Usage: "list existing volumes: `list`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "startID, s",
				Value: "",
				Usage: ": list a range of volume ID's `[--startid <startID>]`",
			},
			cli.StringFlag{
				Name:  "limit, l",
				Value: "",
				Usage: ": limit the number of volumes returned to `[--limit <limit>]`",
			},
			cli.StringFlag{
				Name:  "account, a",
				Value: "",
				Usage: ": only retrieve volumes for the specified accountID `[--account <accountID>]` (not compatible with other options)",
			},
		},
		Action: cmdVolumeList,
	}

	volumeAttachCmd = cli.Command{
		Name:  "attach",
		Usage: "iscsi attach volume to host (requires permissions and iscsiadm): `attach VOLUME-ID`",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "iface",
				Usage: "Device file of iSCSI interface (initiator): `[--iface /dev/p1p2]`",
			},
		},
		Action: cmdVolumeAttach,
	}

	volumeDetachCmd = cli.Command{
		Name:   "detach",
		Usage:  "iscsi detach volume from host (requires permissions and iscsiadm): `detach VOLUME-ID`",
		Action: cmdVolumeDetach,
	}

	volumeAddToVag = cli.Command{
		Name:   "addtovag",
		Usage:  "Add existing Volume to existing Volume Access Group: `addtovag VOLUME-ID VAG-ID`",
		Action: cmdVolumeAddToVag,
	}
)

func cmdVolumeAttach(c *cli.Context) (err error) {
	id := c.Args().First()
	volID, _ := strconv.ParseInt(id, 10, 64)
	v, err := client.GetVolumeByID(volID)
	if err != nil {
		err = errors.New("Failed to find volume for attach")
		return err
	}
	netDev := c.String("iface")
	if c.String("iface") == "" {
		netDev = "default"
	}
	path, device, err := client.AttachVolume(&v, netDev)
	if err != nil {
		fmt.Println("Error encountered while performing iSCSI attach on Volume: ", volID)
		fmt.Println(err)
		return err

	}

	fmt.Println("-------------------------------------------")
	fmt.Println("Succesfully iSCSI Attached Volume:")
	fmt.Println("-------------------------------------------")
	fmt.Println("ID:         ", volID)
	fmt.Println("Path:       ", path)
	fmt.Println("Device:     ", device)
	fmt.Println("-------------------------------------------")
	return err

}

func cmdVolumeDetach(c *cli.Context) (err error) {
	id := c.Args().First()
	volID, _ := strconv.ParseInt(id, 10, 64)
	v, err := client.GetVolumeByID(volID)
	err = client.DetachVolume(v)
	if err != nil {
		fmt.Println("Error encountered while performing iSCSI detach of Volume: ", volID)
		fmt.Println(err)
		return err

	}
	return err
}

func cmdVolumeAddToVag(c *cli.Context) (err error) {
	vID, _ := strconv.ParseInt(c.Args().First(), 10, 64)
	vagID, _ := strconv.ParseInt(c.Args()[1], 10, 64)

	var volIDs []int64
	volIDs = append(volIDs, vID)
	err = client.AddVolumeToAccessGroup(vagID, volIDs)
	if err != nil {
		fmt.Printf("Failed to add volume to VAG ID: %d\n", vagID)
		return err
	}
	fmt.Printf("Succesfully added volume to VAG ID: %d\n", vagID)
	return err
}

func cmdVolumeRollback(c *cli.Context) (err error) {
	var req sfapi.RollbackToSnapshotRequest
	vid, _ := strconv.ParseInt(c.Args().First(), 10, 64)
	sid, _ := strconv.ParseInt(c.Args()[1], 10, 64)

	req.VolumeID = vid
	req.SnapshotID = sid
	_, err = client.RollbackToSnapshot(&req)
	if err != nil {
		fmt.Errorf("failed rollback to snapshot: %+v\n", err)
	}
	return err
}

func cmdVolumeClone(c *cli.Context) (err error) {
	var req sfapi.CloneVolumeRequest
	id, _ := strconv.ParseInt(c.Args().First(), 10, 64)
	name := c.Args()[1]
	if id == 0 || name == "" {
		fmt.Printf("Error, missing arguments to clone cmd")
		// TODO(jdg): Create error object
		return err
	}
	req.VolumeID = id
	req.Name = name
	v, err := client.CloneVolume(&req)
	if err != nil {
		fmt.Println("Error cloning volume: ", err)
		return err
	}
	fmt.Println("-------------------------------------------")
	fmt.Println("Succesfully Cloned Volume:")
	fmt.Println("-------------------------------------------")
	fmt.Println("ID:         ", v.VolumeID)
	fmt.Println("Name:       ", v.Name)
	fmt.Println("Size (GiB): ", v.TotalSize/int64(units.GiB))
	fmt.Println("QoS :       ", "minIOPS:", v.Qos.MinIOPS, "maxIOPS:", v.Qos.MaxIOPS, "burstIOPS:", v.Qos.BurstIOPS)
	fmt.Println("Account:    ", v.AccountID)
	fmt.Println("-------------------------------------------")
	return err
}

func cmdVolumeCreate(c *cli.Context) (err error) {
	var req sfapi.CreateVolumeRequest
	var qos sfapi.QoS
	req.Name = c.Args().First()

	sz := int64(0)
	if c.String("size") == "" && client.DefaultVolSize != 0 {
		sz = client.DefaultVolSize
	} else if c.String("size") != "" {
		sz, _ = units.ParseStrictBytes(c.String("size"))
	} else {
		fmt.Println("You must specify size for volumeCreate")
		return
	}

	account := int64(0)
	if c.String("account") == "" && client.DefaultAccountID != 0 {
		account = client.DefaultAccountID
	} else if c.String("account") != "" {
		account, _ = strconv.ParseInt(c.String("account"), 10, 64)
	} else {
		fmt.Println("You must specify an account for volumeCreate")
		return
	}

	req.TotalSize = sz
	req.AccountID = account
	if c.String("qos") != "" {
		iops := strings.Split(c.String("qos"), ",")
		qos.MinIOPS, _ = strconv.ParseInt(iops[0], 10, 64)
		qos.MaxIOPS, _ = strconv.ParseInt(iops[2], 10, 64)
		qos.BurstIOPS, _ = strconv.ParseInt(iops[2], 10, 64)
		req.Qos = qos
	} else if c.String("type") != "" {
		for _, t := range *client.Config.Types {
			if t.Type == c.String("type") {
				req.Qos = t.QOS
			}
		}
	} else {
	}

	v, err := client.CreateVolume(&req)
	if err != nil {
		fmt.Println("Error creating volume: ", err)
	}
	fmt.Println("-------------------------------------------")
	fmt.Println("Succesfully Created Volume:")
	fmt.Println("-------------------------------------------")
	fmt.Println("ID:         ", v.VolumeID)
	fmt.Println("Name:       ", v.Name)
	fmt.Println("Size (GiB): ", v.TotalSize/int64(units.GiB))
	fmt.Println("QoS :       ", "minIOPS:", v.Qos.MinIOPS, "maxIOPS:", v.Qos.MaxIOPS, "burstIOPS:", v.Qos.BurstIOPS)
	fmt.Println("Account:    ", v.AccountID)
	fmt.Println("-------------------------------------------")

	if c.String("vag") != "" {
		vagID, _ := strconv.ParseInt(c.String("vag"), 10, 64)
		var volIDs []int64
		volIDs = append(volIDs, v.VolumeID)
		err := client.AddVolumeToAccessGroup(vagID, volIDs)
		if err != nil {
			fmt.Printf("Failed to add volume to VAG ID: %d\n", vagID)
			return err
		}
		fmt.Printf("Succesfully added volume to VAG ID: %d\n", vagID)
	}
	return nil
}

func cmdVolumeDelete(c *cli.Context) (err error) {
	volumes := c.String("range")
	if volumes != "" {
		ids := strings.Split(volumes, "-")
		fmt.Println("You've selected to delete volumes: ", ids[0], " through ", ids[1])
		fmt.Print("Are you sure you want to do this [yes/no]: ")
		if confirm() {
			startID, _ := strconv.ParseInt(ids[0], 10, 64)
			endID, _ := strconv.ParseInt(ids[1], 10, 64)
			client.DeleteRange(startID, endID)
		}

	} else {
		for _, arg := range c.Args() {
			vID, _ := strconv.ParseInt(arg, 10, 64)
			client.DeleteVolume(vID)
		}
	}
	return nil
}

func listForAccount(acctID int64) (vols []sfapi.Volume, err error) {
	var req sfapi.ListVolumesForAccountRequest
	req.AccountID = acctID
	return client.ListVolumesForAccount(&req)
}

func listActiveVolumes(req sfapi.ListActiveVolumesRequest) (vols []sfapi.Volume, err error) {
	return client.ListActiveVolumes(&req)
}

func cmdVolumeList(c *cli.Context) (err error) {
	var req sfapi.ListActiveVolumesRequest
	var volumes []sfapi.Volume

	if c.String("account") != "" {
		acctID, _ := strconv.ParseInt(c.String("account"), 10, 64)
		volumes, err = listForAccount(acctID)
	} else {
		if c.String("startID") != "" {
			stID, _ := strconv.ParseInt(c.String("startID"), 10, 64)
			req.StartVolumeID = stID
		}
		if c.String("limit") != "" {
			limit, _ := strconv.ParseInt(c.String("limit"), 10, 64)
			req.Limit = limit
		}
		volumes, err = client.ListActiveVolumes(&req)
	}

	if err != nil {
		fmt.Println(err)
	} else {
		printVolList(volumes)
	}
	return err

}
