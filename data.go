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

type TuneResults struct {
	Data      []byte    `datastore:"data" json:"-"`
	Timestamp time.Time `datastore:"timestamp"`
	Addr      string    `datastore:"addr"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`

	// Fields raised out of the JSON for querying
	UUID  string  `datastore:"uuid"`
	Board string  `datastore:"board"`
	Tau   float64 `datastore:"tau"`

	Key  *datastore.Key   `datastore:"-"`
	Orig *json.RawMessage `datastore:"-"`
}

func (u *TuneResults) setKey(to *datastore.Key) {
	u.Key = to
}

type Keyable interface {
	setKey(*datastore.Key)
}

func (t *TuneResults) compress() error {
	buf := &bytes.Buffer{}
	// this errors only if you give it an invalid level
	w, _ := gzip.NewWriterLevel(buf, gzip.BestCompression)
	_, err := w.Write(t.Data)
	if err != nil {
		return err
	}
	err = w.Close()
	if err == nil && buf.Len() < len(t.Data) {
		t.Data = buf.Bytes()
	}
	return err
}

func (t *TuneResults) uncompress() error {
	if len(t.Data) < 2 {
		return nil
	}
	r, err := gzip.NewReader(bytes.NewReader(t.Data))
	switch err {
	case nil:
	case gzip.ErrHeader:
		return nil
	default:
		return err
	}
	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, r)
	if err == nil {
		t.Data = buf.Bytes()
	}
	return err
}
