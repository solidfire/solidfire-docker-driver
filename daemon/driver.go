package daemon

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/alecthomas/units"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/solidfire/solidfire-docker-driver/sfapi"
)

const Version = "1.3.2"

type SolidFireDriver struct {
	TenantID       int64
	DefaultVolSz   int64
	VagID          int64
	MountPoint     string
	InitiatorIFace string
	Client         *sfapi.Client
	Mutex          *sync.Mutex
}

func verifyConfiguration(cfg *sfapi.Config) {
	// We want to verify we have everything we need to run the Docker driver
	if cfg.TenantName == "" {
		log.Fatal("TenantName required in SolidFire Docker config")
	}
	if cfg.EndPoint == "" {
		log.Fatal("EndPoint required in SolidFire Docker config")
	}
	if cfg.DefaultVolSz == 0 {
		log.Fatal("DefaultVolSz required in SolidFire Docker config")
	}
	if cfg.SVIP == "" {
		log.Fatal("SVIP required in SolidFire Docker config")
	}
}
func New(cfgFile string) SolidFireDriver {
	var tenantID int64
	client, _ := sfapi.NewFromConfig(cfgFile)

	req := sfapi.GetAccountByNameRequest{
		Name: client.DefaultTenantName,
	}
	account, err := client.GetAccountByName(&req)
	if err != nil {
		req := sfapi.AddAccountRequest{
			Username: client.DefaultTenantName,
		}
		actID, nerr := client.AddAccount(&req)
		if nerr != nil {
			log.Fatalf("Failed init, unable to create Tenant (%s): %+v", client.DefaultTenantName, err)
		}
		tenantID = actID
		log.Debug("Set tenantID: ", tenantID)
	} else {
		tenantID = account.AccountID
		log.Debug("Set tenantID: ", tenantID)
	}
	baseMountPoint := "/var/lib/solidfire/mount"
	if client.Config.MountPoint != "" {
		baseMountPoint = client.Config.MountPoint
	}

	iscsiInterface := "default"
	if client.Config.InitiatorIFace != "" {
		iscsiInterface = client.Config.InitiatorIFace
	}

	_, err = os.Lstat(baseMountPoint)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(baseMountPoint, 0755); err != nil {
			log.Fatal("Failed to create Mount directory during driver init: %v", err)
		}
	}

	d := SolidFireDriver{
		TenantID:       tenantID,
		Client:         client,
		Mutex:          &sync.Mutex{},
		DefaultVolSz:   client.DefaultVolSize,
		MountPoint:     client.Config.MountPoint,
		InitiatorIFace: iscsiInterface,
	}
	return d
}

func translateName(name string) string {
	return strings.Replace(name, "_", "-", -1)
}

func NewSolidFireDriverFromConfig(c *sfapi.Config) SolidFireDriver {
	var tenantID int64

	client, _ := sfapi.NewFromConfig("")
	req := sfapi.GetAccountByNameRequest{
		Name: c.TenantName,
	}

	account, err := client.GetAccountByName(&req)
	if err != nil {
		req := sfapi.AddAccountRequest{
			Username: c.TenantName,
		}
		tenantID, err = client.AddAccount(&req)
		if err != nil {
			log.Fatal("Failed to initialize solidfire driver while creating tenant: ", err)
		}
	} else {
		tenantID = account.AccountID
	}

	baseMountPoint := "/var/lib/solidfire/mount"
	if c.MountPoint != "" {
		baseMountPoint = c.MountPoint
	}

	iscsiInterface := "default"
	if c.InitiatorIFace != "" {
		iscsiInterface = c.InitiatorIFace
	}

	if c.Types != nil {
		client.VolumeTypes = c.Types
	}

	defaultVolSize := int64(1)
	if c.DefaultVolSz != 0 {
		defaultVolSize = c.DefaultVolSz
	}

	_, err = os.Lstat(baseMountPoint)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(baseMountPoint, 0755); err != nil {
			log.Fatal("Failed to create Mount directory during driver init: %v", err)
		}
	}

	d := SolidFireDriver{
		TenantID:       tenantID,
		Client:         client,
		Mutex:          &sync.Mutex{},
		DefaultVolSz:   defaultVolSize,
		MountPoint:     c.MountPoint,
		InitiatorIFace: iscsiInterface,
	}
	log.Debugf("Driver initialized with the following settings:\n%+v\n", d)
	log.Info("Succesfuly initialized SolidFire Docker driver")
	return d
}

func formatOpts(r volume.Request) {
	// NOTE(jdg): For now we just want to minimize issues like case usage for
	// the two basic opts most used (size and type).  Going forward we can add
	// all sorts of things here based on what we decide to add as valid opts
	// during create and even other calls
	for k, v := range r.Options {
		if strings.EqualFold(k, "size") {
			r.Options["size"] = v
		} else if strings.EqualFold(k, "type") {
			r.Options["type"] = v
		} else if strings.EqualFold(k, "qos") {
			r.Options["qos"] = v
		} else if strings.EqualFold(k, "from") {
			r.Options["from"] = translateName(v)
		} else if strings.EqualFold(k, "fromSnapshot") {
			r.Options["fromSnapshot"] = translateName(v)
		}
	}
}

func (d SolidFireDriver) Create(r volume.Request) volume.Response {
	log.Infof("Create volume %s on %s\n", r.Name, "solidfire")
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	theName := translateName(r.Name)
	log.Debugf("GetVolumeByName: %s, %d", r.Name, d.TenantID)
	log.Debugf("Options passed in to create: %+v", r.Options)
	v, err := d.Client.GetVolumeByName(theName, d.TenantID)
	if err == nil && v.VolumeID != 0 {
		log.Infof("Found existing Volume by Name: %s", r.Name)
		return volume.Response{}
	}
	formatOpts(r)
	log.Debugf("Options after conversion: %+v", r.Options)
	var vsz int64
	if r.Options["size"] != "" {
		s, _ := strconv.ParseInt(r.Options["size"], 10, 64)
		log.Info("Received size request in Create: ", s)
		vsz = int64(units.GB) * s
	} else {
		// NOTE(jdg): We need to cleanup the conversions and such when we read
		// in from the config file, it's sort of ugly.  BUT, just remember that
		// when we pull the value from d.DefaultVolSz it's already been
		// multiplied
		vsz = d.DefaultVolSz
		log.Info("Creating with default size of: ", vsz)
	}
	// If 'from' is specified, this becomes a clone request
	_, from := r.Options["from"]
	_, fromSnapshot := r.Options["fromSnapshot"]
	if from || fromSnapshot {
		rsp := d.CloneVolume(r, vsz)
		return rsp
	}
	var req sfapi.CreateVolumeRequest
	var meta = map[string]string{"platform": "Docker-SFVP",
		"SFVP-Version": Version,
		"DockerName":   r.Name}
	req.Qos = d.Client.MergeQoS(r.Options["type"], r.Options["qos"])
	req.TotalSize = vsz
	req.AccountID = d.TenantID
	req.Name = theName
	req.Attributes = meta
	_, err = d.Client.CreateVolume(&req)
	if err != nil {
		return volume.Response{Err: err.Error()}
	}
	return volume.Response{}
}

func (d SolidFireDriver) Remove(r volume.Request) volume.Response {
	log.Info("Remove/Delete Volume: ", r.Name)
	theName := translateName(r.Name)
	v, err := d.Client.GetVolumeByName(theName, d.TenantID)
	if err != nil {
		log.Error("Failed to retrieve volume named ", r.Name, "during Remove operation: ", err)
		return volume.Response{Err: err.Error()}
	}
	d.Client.DetachVolume(v)
	err = d.Client.DeleteVolume(v.VolumeID)
	if err != nil {
		// FIXME(jdg): Check if it's a "DNE" error in that case we're golden
		log.Error("Error encountered during delete: ", err)
	}
	return volume.Response{}
}

func (d SolidFireDriver) Path(r volume.Request) volume.Response {
	log.Info("Retrieve path info for volume: ", r.Name)
	path := filepath.Join(d.MountPoint, r.Name)
	log.Debug("Path reported as: ", path)
	return volume.Response{Mountpoint: path}
}

func (d SolidFireDriver) Mount(r volume.MountRequest) volume.Response {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	log.Infof("Mounting volume %s on %s\n", r.Name, "solidfire")
	theName := translateName(r.Name)
	v, err := d.Client.GetVolumeByName(theName, d.TenantID)
	if err != nil {
		log.Error("Failed to retrieve volume by name in mount operation: ", r.Name)
		return volume.Response{Err: err.Error()}
	}
	path, device, err := d.Client.AttachVolume(&v, d.InitiatorIFace)
	if path == "" || device == "" && err == nil {
		log.Error("Missing path or device, but err not set?")
		log.Debug("Path: ", path, ",Device: ", device)
		return volume.Response{Err: err.Error()}

	}
	if err != nil {
		log.Errorf("Failed to perform iscsi attach of volume %s: %v", r.Name, err)
		return volume.Response{Err: err.Error()}
	}
	log.Debugf("Attached volume at (path, devfile): %s, %s", path, device)
	if sfapi.GetFSType(device) == "" {
		//TODO(jdg): Enable selection of *other* fs types
		err := sfapi.FormatVolume(device, "ext4")
		if err != nil {
			log.Error("Failed to format device: ", device)
			return volume.Response{Err: err.Error()}
		}
	}
	if sfapi.Mount(device, d.MountPoint+"/"+r.Name) != nil {
		log.Error("Failed to mount volume: ", r.Name)
		return volume.Response{Err: err.Error()}
	}
	return volume.Response{Mountpoint: d.MountPoint + "/" + r.Name}
}

func (d SolidFireDriver) Unmount(r volume.UnmountRequest) volume.Response {
	log.Info("Unmounting volume: ", r.Name)
	theName := translateName(r.Name)
	sfapi.Umount(filepath.Join(d.MountPoint, r.Name))
	v, err := d.Client.GetVolumeByName(theName, d.TenantID)
	if err != nil {
		return volume.Response{Err: err.Error()}
	}
	d.Client.DetachVolume(v)
	return volume.Response{}
}

func (d SolidFireDriver) Get(r volume.Request) volume.Response {
	log.Info("Get volume: ", r.Name)
	path := filepath.Join(d.MountPoint, r.Name)
	theName := translateName(r.Name)
	v, err := d.Client.GetVolumeByName(theName, d.TenantID)
	if err != nil {
		log.Error("Failed to retrieve volume named ", r.Name, "during Get operation: ", err)
		return volume.Response{Err: err.Error()}
	}
	return volume.Response{Volume: &volume.Volume{Name: v.Name, Mountpoint: path}}
}

func (d SolidFireDriver) List(r volume.Request) volume.Response {
	log.Info("Get volume: ", r.Name)
	path := filepath.Join(d.MountPoint, r.Name)
	var vols []*volume.Volume
	var req sfapi.ListVolumesForAccountRequest
	req.AccountID = d.TenantID
	vlist, err := d.Client.ListVolumesForAccount(&req)
	if err != nil {
		log.Error("Failed to retrieve volume list:", err)
		return volume.Response{Err: err.Error()}
	}

	for _, v := range vlist {
		if v.Status == "active" && v.AccountID == d.TenantID {
			vols = append(vols, &volume.Volume{Name: v.Name, Mountpoint: path})
		}
	}
	return volume.Response{Volumes: vols}
}

func (d SolidFireDriver) Capabilities(r volume.Request) volume.Response {
	return volume.Response{Capabilities: volume.Capability{Scope: "global"}}
}

func (d SolidFireDriver) CloneVolume(r volume.Request, vsz int64) volume.Response {
	log.Infof("Clone volume %s on %s\n", r.Name, "solidfire")
	var req sfapi.CloneVolumeRequest
	var meta = map[string]string{"platform": "Docker-SFVP",
		"SFVP-Version": Version,
		"DockerName":   r.Name}
	theName := translateName(r.Name)
	if r.Options["fromSnapshot"] != "" {
		// if we have fromSnapshot we can get the volumeID
		snap, err := d.Client.GetSnapshot(0, r.Options["fromSnapshot"])
		if err != nil {
			log.Error("Failed to retrieve Snapshot ID:", err)
			return volume.Response{Err: err.Error()}
		}
		req.SnapshotID = snap.SnapshotID
		req.VolumeID = snap.VolumeID
		if snap.TotalSize > vsz {
			req.NewSize = snap.TotalSize
		} else {
			req.NewSize = vsz
		}
		meta["From-Snapshot"] = strconv.FormatInt(req.SnapshotID, 10)
	} else {
		sv, err := d.Client.GetVolumeByName(r.Options["from"], d.TenantID)
		if err != nil {
			log.Error("Failed to retrieve Src volume:", err)
			return volume.Response{Err: err.Error()}
		}
		req.VolumeID = sv.VolumeID
		if sv.TotalSize > vsz {
			req.NewSize = sv.TotalSize
		} else {
			req.NewSize = vsz
		}
		meta["From-Volume"] = strconv.FormatInt(req.VolumeID, 10)
	}
	req.Name = theName
	req.NewAccountID = d.TenantID
	req.Attributes = meta
	nv, err := d.Client.CloneVolume(&req)
	if err != nil {
		log.Error("Failed to clone volume:", err)
		return volume.Response{Err: err.Error()}
	}
	var qos sfapi.QoS
	if qos = d.Client.MergeQoS(r.Options["type"], r.Options["qos"]); qos != (sfapi.QoS{}) {
		var modreq sfapi.ModifyVolumeRequest
		modreq.VolumeID = nv.VolumeID
		modreq.Qos = qos
		err := d.Client.ModifyVolume(&modreq)
		if err != nil {
			log.Error("Failed to update QoS on cloned volume:", err)
			return volume.Response{Err: err.Error()}
		}
	}
	return volume.Response{}
}
