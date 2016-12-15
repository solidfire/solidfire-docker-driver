package sfapi

type APIError struct {
	Id    int `json:"id"`
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Name    string `json:"name"`
	} `json:"error"`
}

type QoS struct {
	MinIOPS   int64 `json:"minIOPS,omitempty"`
	MaxIOPS   int64 `json:"maxIOPS,omitempty"`
	BurstIOPS int64 `json:"burstIOPS,omitempty"`
	BurstTime int64 `json:"-"`
}

type VolumePair struct {
	ClusterPairID    int64  `json:"clusterPairID"`
	RemoteVolumeID   int64  `json:"remoteVolumeID"`
	RemoteSliceID    int64  `json:"remoteSliceID"`
	RemoteVolumeName string `json:"remoteVolumeName"`
	VolumePairUUID   string `json:"volumePairUUID"`
}

type Volume struct {
	VolumeID           int64        `json:"volumeID"`
	Name               string       `json:"name"`
	AccountID          int64        `json:"accountID"`
	CreateTime         string       `json:"createTime"`
	Status             string       `json:"status"`
	Access             string       `json:"access"`
	Enable512e         bool         `json:"enable512e"`
	Iqn                string       `json:"iqn"`
	ScsiEUIDeviceID    string       `json:"scsiEUIDeviceID"`
	ScsiNAADeviceID    string       `json:"scsiNAADeviceID"`
	Qos                QoS          `json:"qos"`
	VolumeAccessGroups []int64      `json:"volumeAccessGroups"`
	VolumePairs        []VolumePair `json:"volumePairs"`
	DeleteTime         string       `json:"deleteTime"`
	PurgeTime          string       `json:"purgeTime"`
	SliceCount         int64        `json:"sliceCount"`
	TotalSize          int64        `json:"totalSize"`
	BlockSize          int64        `json:"blockSize"`
	VirtualVolumeID    string       `json:"virtualVolumeID"`
	Attributes         interface{}  `json:"attributes"`
}

type Snapshot struct {
	SnapshotID int64       `json:"snapshotID"`
	VolumeID   int64       `json:"volumeID"`
	Name       string      `json:"name"`
	Checksum   string      `json:"checksum"`
	Status     string      `json:"status"`
	TotalSize  int64       `json:"totalSize"`
	GroupID    int64       `json:"groupID"`
	CreateTime string      `json:"createTime"`
	Attributes interface{} `json:"attributes"`
}

type ListVolumesForAccountRequest struct {
	AccountID int64 `json:"accountID"`
}

type ListActiveVolumesRequest struct {
	StartVolumeID int64 `json:"startVolumeID"`
	Limit         int64 `json:"limit"`
}

type ListVolumesResult struct {
	Id     int `json:"id"`
	Result struct {
		Volumes []Volume `json:"volumes"`
	} `json:"result"`
}

type CreateVolumeRequest struct {
	Name       string      `json:"name"`
	AccountID  int64       `json:"accountID"`
	TotalSize  int64       `json:"totalSize"`
	Enable512e bool        `json:"enable512e"`
	Qos        QoS         `json:"qos,omitempty"`
	Attributes interface{} `json:"attributes"`
}

type CreateVolumeResult struct {
	Id     int `json:"id"`
	Result struct {
		VolumeID int64 `json:"volumeID"`
	} `json:"result"`
}

type CloneVolumeRequest struct {
	VolumeID     int64       `json:"volumeID"`
	Name         string      `json:"name"`
	NewAccountID int64       `json:"newAccountID,omitempty"`
	NewSize      int64       `json:"newSize,omitempty"`
	Access       string      `json:"access,omitempty"`
	SnapshotID   int64       `json:"snapshotID,omitempty"`
	Attributes   interface{} `json:"attributes"`
}

type CloneVolumeResult struct {
	Id     int `json:"id"`
	Result struct {
		CloneID     int64 `json:"cloneID"`
		VolumeID    int64 `json:"volumeID"`
		AsyncHandle int64 `json:"asyncHandle"`
	} `json:"result"`
}

type ModifyVolumeRequest struct {
	VolumeID   int64       `json:"volumeID"`
	AccountID  int64       `json:"accountID,omitempty"`
	TotalSize  int64       `json:"totalSize,omitempty"`
	Qos        QoS         `json:"qos,omitempty"`
	Attributes interface{} `json:"attributes,omitempty"`
}

type CreateSnapshotRequest struct {
	VolumeID                int64       `json:"volumeID"`
	SnapshotID              int64       `json:"snapshotID"`
	Name                    string      `json:"name"`
	EnableRemoteReplication bool        `json:"enableRemoteReplication"`
	Retention               string      `json:"retention"`
	Attributes              interface{} `json:"attributes"`
}

type CreateSnapshotResult struct {
	Id     int `json:"id"`
	Result struct {
		SnapshotID int64  `json:"snapshotID"`
		Checksum   string `json:"checksum"`
	} `json:"result"`
}

type DeleteVolumeRequest struct {
	VolumeID int64 `json:"volumeID"`
}

type ISCSITarget struct {
	Ip        string
	Port      string
	Portal    string
	Iqn       string
	Lun       string
	Device    string
	Discovery string
}

type ListSnapshotsRequest struct {
	VolumeID int64 `json:"volumeID"`
}

type ListSnapshotsResult struct {
	Id     int `json:"id"`
	Result struct {
		Snapshots []Snapshot `json:"snapshots"`
	} `json:"result"`
}

type RollbackToSnapshotRequest struct {
	VolumeID         int64       `json:"volumeID"`
	SnapshotID       int64       `json:"snapshotID"`
	SaveCurrentState bool        `json:"saveCurrentState"`
	Name             string      `json:"name"`
	Attributes       interface{} `json:"attributes"`
}

type RollbackToSnapshotResult struct {
	Id     int `json:"id"`
	Result struct {
		Checksum   string `json:"checksum"`
		SnapshotID int64  `json:"snapshotID"`
	} `json:"result"`
}

type DeleteSnapshotRequest struct {
	SnapshotID int64 `json:"snapshotID"`
}

type AddVolumesToVolumeAccessGroupRequest struct {
	VolumeAccessGroupID int64   `json:"volumeAccessGroupID"`
	Volumes             []int64 `json:"volumes"`
}

type CreateVolumeAccessGroupRequest struct {
	Name       string   `json:"name"`
	Volumes    []int64  `json:"volumes,omitempty"`
	Initiators []string `json:"initiators,omitempty"`
}

type CreateVolumeAccessGroupResult struct {
	Id     int `json:"id"`
	Result struct {
		VagID int64 `json:"volumeAccessGroupID"`
	} `json:"result"`
}

type AddInitiatorsToVolumeAccessGroupRequest struct {
	Initiators []string `json:"initiators"`
	VAGID      int64    `json:"volumeAccessGroupID"`
}

type ListVolumeAccessGroupsRequest struct {
	StartVAGID int64 `json:"startVolumeAccessGroupID,omitempty"`
	Limit      int64 `json:"limit,omitempty"`
}

type ListVolumesAccessGroupsResult struct {
	Id     int `json:"id"`
	Result struct {
		Vags []VolumeAccessGroup `json:"volumeAccessGroups"`
	} `json:"result"`
}

type EmptyResponse struct {
	Id     int `json:"id"`
	Result struct {
	} `json:"result"`
}

type VolumeAccessGroup struct {
	Initiators     []string    `json:"initiators"`
	Attributes     interface{} `json:"attributes"`
	DeletedVolumes []int64     `json:"deletedVolumes"`
	Name           string      `json:"name"`
	VAGID          int64       `json:"volumeAccessGroupID"`
	Volumes        []int64     `json:"volumes"`
}

type GetAccountByNameRequest struct {
	Name string `json:"username"`
}

type GetAccountByIDRequest struct {
	AccountID int64 `json:"accountID"`
}

type GetAccountResult struct {
	Id     int `json:"id"`
	Result struct {
		Account Account `json:"account"`
	} `json:"result"`
}

type Account struct {
	AccountID       int64       `json:"accountID,omitempty"`
	Username        string      `json:"username,omitempty"`
	Status          string      `json:"status,omitempty"`
	Volumes         []int64     `json:"volumes,omitempty"`
	InitiatorSecret string      `json:"initiatorSecret,omitempty"`
	TargetSecret    string      `json:"targetSecret,omitempty"`
	Attributes      interface{} `json:"attributes,omitempty"`
}

type AddAccountRequest struct {
	Username        string      `json:"username"`
	InitiatorSecret string      `json:"initiatorSecret,omitempty"`
	TargetSecret    string      `json:"targetSecret,omitempty"`
	Attributes      interface{} `json:"attributes,omitempty"`
}

type AddAccountResult struct {
	Id     int `json:"id"`
	Result struct {
		AccountID int64 `json:"accountID"`
	} `json:"result"`
}
