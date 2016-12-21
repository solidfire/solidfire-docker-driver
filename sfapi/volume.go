package sfapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

func (c *Client) ListVolumesForAccount(listReq *ListVolumesForAccountRequest) (volumes []Volume, err error) {
	response, err := c.Request("ListVolumesForAccount", listReq, newReqID())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	var result ListVolumesResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		log.Fatal(err)
		return nil, err
	}
	volumes = result.Result.Volumes
	return volumes, err
}

func (c *Client) GetVolumeByID(volID int64) (v Volume, err error) {
	var req ListActiveVolumesRequest
	req.StartVolumeID = volID
	req.Limit = 1
	volumes, err := c.ListActiveVolumes(&req)
	if err != nil {
		return v, err
	}
	if len(volumes) < 1 {
		return Volume{}, fmt.Errorf("Failed to find volume with ID: %d", volID)
	}
	return volumes[0], nil
}

func (c *Client) GetVolumeByName(n string, acctID int64) (v Volume, err error) {
	vols, err := c.GetVolumesByName(n, acctID)
	if err == nil && len(vols) == 1 {
		return vols[0], nil
	}

	if len(vols) > 1 {
		err = fmt.Errorf("Found more than one Volume with Name: %s for Account: %d", n, acctID)
	} else if len(vols) < 1 {
		err = fmt.Errorf("Failed to find any Volumes with Name: %s for Account: %d", n, acctID)
	}
	return v, err
}

func (c *Client) GetVolumesByName(sfName string, acctID int64) (v []Volume, err error) {
	var listReq ListVolumesForAccountRequest
	var foundVolumes []Volume
	listReq.AccountID = acctID
	volumes, err := c.ListVolumesForAccount(&listReq)
	if err != nil {
		log.Error("Error retrieving volumes: ", err)
		return foundVolumes, err
	}
	for _, vol := range volumes {
		if vol.Name == sfName && vol.Status == "active" {
			foundVolumes = append(foundVolumes, vol)
		}
	}
	if len(foundVolumes) > 1 {
		log.Warningf("Found more than one volume with the name: %s\n%+v", sfName, foundVolumes)
	}
	if len(foundVolumes) == 0 {
		return foundVolumes, fmt.Errorf("Failed to find any volumes by the name of: %s for this account: %d", sfName, acctID)
	}
	return foundVolumes, nil
}

func (c *Client) ListActiveVolumes(listVolReq *ListActiveVolumesRequest) (volumes []Volume, err error) {
	response, err := c.Request("ListActiveVolumes", listVolReq, newReqID())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	var result ListVolumesResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		log.Fatal(err)
		return nil, err
	}
	volumes = result.Result.Volumes
	return volumes, err
}

func (c *Client) CloneVolume(req *CloneVolumeRequest) (vol Volume, err error) {
	response, err := c.Request("CloneVolume", req, newReqID())
	var result CloneVolumeResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		log.Fatal(err)
		return Volume{}, err
	}

	wait := 0
	multiplier := 1
	for wait < 10 {
		wait += wait
		vol, err = c.GetVolumeByID(result.Result.VolumeID)
		if err == nil {
			break
		}
		time.Sleep(time.Second * time.Duration(multiplier))
		multiplier *= wait
	}
	return
}

func (c *Client) CreateVolume(createReq *CreateVolumeRequest) (vol Volume, err error) {
	response, err := c.Request("CreateVolume", createReq, newReqID())
	if err != nil {
		return Volume{}, err
	}
	var result CreateVolumeResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		log.Fatal(err)
		return Volume{}, err
	}

	vol, err = c.GetVolumeByID(result.Result.VolumeID)
	return
}

func (c *Client) AddVolumeToAccessGroup(groupID int64, volIDs []int64) (err error) {
	req := &AddVolumesToVolumeAccessGroupRequest{
		VolumeAccessGroupID: groupID,
		Volumes:             volIDs,
	}
	_, err = c.Request("AddVolumesToVolumeAccessGroup", req, newReqID())
	if err != nil {
		log.Error("Failed to add volume(s) to VAG %d: ", groupID)
		return err
	}
	return err
}

func (c *Client) DeleteRange(startID, endID int64) {
	idx := startID
	for idx < endID {
		c.DeleteVolume(idx)
	}
	return
}

func (c *Client) DeleteVolume(volumeID int64) (err error) {
	// TODO(jdg): Add options like purge=True|False, range, ALL etc
	var req DeleteVolumeRequest
	req.VolumeID = volumeID
	_, err = c.Request("DeleteVolume", req, newReqID())
	if err != nil {
		log.Error("Failed to delete volume ID: ", volumeID)
		return err
	}
	return
}

func (c *Client) DetachVolume(v Volume) (err error) {
	if c.SVIP == "" {
		err = errors.New("Unable to perform iSCSI actions without setting SVIP")
		return
	}
	tgt := &ISCSITarget{
		Ip:     c.SVIP,
		Portal: c.SVIP,
		Iqn:    v.Iqn,
	}
	err = iscsiDisableDelete(tgt)
	return
}

func (c *Client) AttachVolume(v *Volume, iface string) (path, device string, err error) {
	var req GetAccountByIDRequest
	path = "/dev/disk/by-path/ip-" + c.SVIP + "-iscsi-" + v.Iqn + "-lun-0"

	if c.SVIP == "" {
		err = errors.New("Unable to perform iSCSI actions without setting SVIP")
		log.Error(err)
		return path, device, err
	}

	if iscsiSupported() == false {
		err := errors.New("Unable to attach, open-iscsi tools not found on host")
		log.Error(err)
		return path, device, err
	}

	req.AccountID = v.AccountID
	a, err := c.GetAccountByID(&req)
	if err != nil {
		log.Error("Failed to get account ", v.AccountID, ": ", err)
		return path, device, err
	}

	// Make sure it's not already attached
	if waitForPathToExist(path, 1) {
		log.Debug("Get device file from path: ", path)
		device = strings.TrimSpace(getDeviceFileFromIscsiPath(path))
		return path, device, nil
	}

	err = LoginWithChap(v.Iqn, c.SVIP, a.Username, a.InitiatorSecret, iface)
	if err != nil {
		log.Error(err)
		return path, device, err
	}
	if waitForPathToExist(path, 5) {
		device = strings.TrimSpace(getDeviceFileFromIscsiPath(path))
		return path, device, nil
	}
	return path, device, nil
}

func (c *Client) ModifyVolume(modReq *ModifyVolumeRequest) (err error) {
	_, err = c.Request("ModifyVolume", modReq, newReqID())
	if err != nil {
		return err
	}
	// only thing that comes back from this is the curve, which is not used
	return nil
}
