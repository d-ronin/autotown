package statstore

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"time"

	"google.golang.org/appengine/datastore"
)

type CrashData struct {
	Comment   string    `datastore:"comment"`
	Directory string    `datastore:"dir"`
	CrashFile string    `datastore:"file"`
	Timestamp time.Time `datastore:"timestamp"`
	Branch    string    `datastore:"branch"`
	Commit    string    `datastore:"commit"`
	Dirty     bool      `datastore:"dirty"`
	Tag       string    `datastore:"tag"`
	Addr      string    `datastore:"addr"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`
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

func NewStat(addr, data string) TuneResults {
	return TuneResults{
		Data:      []byte(data),
		Timestamp: time.Now(),
		Addr:      addr,
	}
}
