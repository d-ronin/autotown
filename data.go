package autotown

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"time"

	"google.golang.org/appengine/datastore"
)

var crashNameMap = map[string]string{
	"directory":   "dir",
	"currentOS":   "os",
	"currentArch": "arch",
}

type CrashData struct {
	properties map[string]interface{}

	Key *datastore.Key `datastore:"-"`
}

func (c *CrashData) Load(ps []datastore.Property) error {
	c.properties = map[string]interface{}{}
	for _, p := range ps {
		c.properties[p.Name] = p.Value
	}
	return nil
}

func (c *CrashData) Save() ([]datastore.Property, error) {
	rv := []datastore.Property{}
	for k, v := range c.properties {
		n := crashNameMap[k]
		if n == "" {
			n = k
		}
		rv = append(rv, datastore.Property{
			Name:  n,
			Value: v,
		})
	}
	return rv, nil
}

func (c *CrashData) MarshalJSON() ([]byte, error) {
	c.properties["Key"] = c.Key
	defer delete(c.properties, "Key")
	return json.Marshal(c.properties)
}

func (c *CrashData) setKey(to *datastore.Key) {
	c.Key = to
}

type timestampedTau struct {
	Tau       float64        `json:"tau"`
	Timestamp time.Time      `json:"timestamp"`
	Key       *datastore.Key `json:"key"`
}

type TuneResults struct {
	Data      []byte    `datastore:"data" json:"-"`
	Timestamp time.Time `datastore:"timestamp"`
	Addr      string    `datastore:"addr" json:"-"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`

	// Fields raised out of the JSON for querying
	UUID  string  `datastore:"uuid", json:"-"`
	Board string  `datastore:"board"`
	Tau   float64 `datastore:"tau"`

	Key  *datastore.Key   `datastore:"-"`
	Orig *json.RawMessage `datastore:"-" json:",omitempty"`

	Older []timestampedTau `datastore:"-" json:"older,omitempty"`

	Experimental interface{} `datastore:"-" json:"experimental,omitempty"`
}

func (u *TuneResults) setKey(to *datastore.Key) {
	u.Key = to
}

type Keyable interface {
	setKey(*datastore.Key)
}

func gz(d []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	// this errors only if you give it an invalid level
	w, _ := gzip.NewWriterLevel(buf, gzip.BestCompression)
	_, err := w.Write(d)
	if err != nil {
		return d, err
	}
	err = w.Close()
	if err == nil && buf.Len() < len(d) {
		d = buf.Bytes()
	}
	return d, err
}

func ungz(d []byte) ([]byte, error) {
	if len(d) < 2 {
		return d, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(d))
	switch err {
	case nil:
	case gzip.ErrHeader:
		return d, nil
	default:
		return d, err
	}
	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, r)
	return buf.Bytes(), err
}

func (t *TuneResults) compress() error {
	d, err := gz(t.Data)
	if err != nil {
		return err
	}
	t.Data = d
	return nil
}

func (t *TuneResults) uncompress() error {
	d, err := ungz(t.Data)
	if err != nil {
		return err
	}
	t.Data = d
	return nil
}

type UsageStat struct {
	Data      []byte    `datastore:"data" json:"-"`
	Timestamp time.Time `datastore:"timestamp"`
	Addr      string    `datastore:"addr"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`
}

func (t *UsageStat) compress() error {
	d, err := gz(t.Data)
	if err != nil {
		return err
	}
	t.Data = d
	return nil
}

func (t *UsageStat) uncompress() error {
	d, err := ungz(t.Data)
	if err != nil {
		return err
	}
	t.Data = d
	return nil
}

type FoundController struct {
	UUID        string `datastore:"uuid"`
	Count       int    `datastore:"count"`
	HardwareRev int    `datastore:"hardware_rev"`
	Name        string `datastore:"name"`
	GitHash     string `datastore:"git_hash"`
	GitTag      string `datastore:"git_tag"`
	UAVOHash    string `datastore:"uavo_hash"`

	GCSOS      string `datastore:"gcs_os"`
	GCSArch    string `datastore:"gss_arch"`
	GCSVersion string `datastore:"gcs_version"`

	Addr      string    `datastore:"addr"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`
	Oldest    time.Time `datastore:"oldest_timestamp"`
	Timestamp time.Time `datastore:"timestamp"`
}
