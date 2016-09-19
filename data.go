package autotown

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/search"
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
		if os, ok := p.Value.(string); ok && p.Name == "os" {
			c.properties["os_abbrev"] = abbrevOS(os)
		}
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
		if n == "os_abbrev" {
			continue
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

type TuneDoc struct {
	Timestamp    time.Time          `search:"ts" json:"ts"`
	Board        search.Atom        `search:"board" json:"board"`
	VehicleType  search.Atom        `search:"vtype" json:"vtype"`
	Observation  string             `search:"observation" json:"observation"`
	Tau          float64            `search:"tau" json:"tau"`
	Location     appengine.GeoPoint `search:"geo" json:"geo"`
	LocationText string             `search:"location" json:"location"`
	Weight       float64            `search:"weight" json:"weight"`
	Size         float64            `search:"size" json:"size"`
	Cells        float64            `search:"cells" json:"cells"`
	UUID         string             `search:"uuid" json:"uuid"`
	Config       string             `search:"config" json:"-"`

	ID string `search:-,json:"key"`
}

func indexDoc(c context.Context, tune *TuneResults) error {
	doc := &TuneDoc{
		Timestamp:    tune.Timestamp,
		Board:        search.Atom(canonicalBoard(tune.Board)),
		VehicleType:  search.Atom(jptrs(c, tune.Orig, "/vehicle/type")),
		Observation:  jptrs(c, tune.Orig, "/userObservations"),
		Tau:          tune.Tau * 1000,
		Location:     appengine.GeoPoint{tune.Lat, tune.Lon},
		LocationText: tune.City + " " + tune.Region + " " + tune.Country,
		Weight:       jptrf(c, tune.Orig, "/vehicle/weight"),
		Size:         jptrf(c, tune.Orig, "/vehicle/size"),
		Cells:        jptrf(c, tune.Orig, "/vehicle/batteryCells"),
		Config:       string(jraw(c, tune.Orig, "/rawSettings")),
		UUID:         tune.UUID,
	}

	log.Debugf(c, "Storing doc: %#v", doc)
	index, err := search.Open("tunes")
	if err != nil {
		return err
	}
	_, err = index.Put(c, tune.Key.Encode(), doc)
	if err != nil {
		return err
	}
	return nil
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

	Orig *json.RawMessage `datastore:"-" json:",omitempty"`
	Key  *datastore.Key   `datastore:"-"`
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

func indexUsage(c context.Context, k string, u *UsageStat) error {
	index, err := search.Open("usage")
	if err != nil {
		return err
	}
	udoc := &UsageDoc{u, nil}
	_, err = index.Put(c, k, udoc)
	return err
}

type UsageDoc struct {
	s *UsageStat
	m map[string]interface{}
}

type uuidboard struct {
	UUID  string `json:"uuid"`
	Board string `json:"board"`
}

func (u *UsageDoc) Load(fields []search.Field, md *search.DocumentMetadata) error {
	u.m = map[string]interface{}{}
	var uuids, boards []string
	for _, f := range fields {
		switch f.Name {
		case "uuid":
			uuids = append(uuids, f.Value.(string))
		case "name":
			boards = append(boards, f.Value.(string))
		default:
			u.m[f.Name] = f.Value
		}
	}
	if len(uuids) == len(boards) && len(uuids) > 0 {
		var bu []uuidboard
		for i := range boards {
			bu = append(bu, uuidboard{uuids[i], boards[i]})
		}
		u.m["boards"] = bu
	}
	return nil
}

func (u *UsageDoc) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.m)
}

func (u *UsageDoc) Save() ([]search.Field, *search.DocumentMetadata, error) {
	fields := []search.Field{
		{Name: "timestamp", Value: u.s.Timestamp},
		{Name: "id", Value: u.s.Key.Encode()},
		{Name: "geo", Value: appengine.GeoPoint{u.s.Lat, u.s.Lon}},
		{Name: "location", Value: u.s.City + " " + u.s.Region + " " + u.s.Country},
	}

	d, err := ungz(u.s.Data)
	if err != nil {
		d = u.s.Data
	}

	var o struct {
		Boards []struct {
			UUID string
			Name string
		} `json:"boardsSeen"`
		DebugLog []struct {
			File, Function, Level, Message string
		}
		OS      string `json:"currentOS"`
		Version string `json:"gcs_version"`
	}
	if err := json.Unmarshal(d, &o); err != nil {
		// Logging seems to fail me here, and I don't
		// necessarily want to fail the whole thing.
	} else {
		fields = append(fields, search.Field{Name: "os", Value: o.OS})
		fields = append(fields, search.Field{Name: "version", Value: o.Version})

		seen := map[string]bool{}
		for _, s := range o.Boards {
			if seen[s.UUID] {
				continue
			}
			seen[s.UUID] = true
			fields = append(fields, search.Field{Name: "uuid", Value: s.UUID})
			fields = append(fields, search.Field{Name: "name", Value: s.Name})
		}

		maxLvl := 0.0
		for _, d := range o.DebugLog {
			lvl := 0.0
			switch d.Level {
			case "debug":
				lvl = 1.0
			case "info":
				lvl = 2.0
			case "warning":
				lvl = 3.0
			case "critical":
				lvl = 4.0
			case "fatal":
				lvl = 5.0
			}
			if lvl > maxLvl {
				maxLvl = lvl
			}
			fields = append(fields, search.Field{Name: "debug_file", Value: d.File})
			fields = append(fields, search.Field{Name: "debug_func", Value: d.Function})
			fields = append(fields, search.Field{Name: "debug_msg", Value: d.Message})
		}

		fields = append(fields, search.Field{Name: "debug_lvl", Value: maxLvl})
	}

	meta := &search.DocumentMetadata{}
	return fields, meta, nil
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
