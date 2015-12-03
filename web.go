package statstore

import (
	"bytes"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-jsonpointer"

	"appengine"
	"appengine/datastore"
	_ "appengine/remote_api"
	"appengine/taskqueue"
)

var templates *template.Template

func init() {
	http.HandleFunc("/storeTune", handleStoreTune)
	http.HandleFunc("/asyncStoreTune", handleAsyncStoreTune)
	http.HandleFunc("/exportTunes", handleExportTunes)
}

func handleStoreTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	rawJson := json.RawMessage{}
	if err := json.NewDecoder(r.Body).Decode(&rawJson); err != nil {
		c.Infof("Error handling input JSON: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	fields := &struct {
		UUID    string `json:"uniqueId"`
		Vehicle struct {
			Firmware struct {
				Board, Commit, Tag string
			}
		}
		Identification struct {
			Tau float64
		}
	}{}

	if err := json.Unmarshal([]byte(rawJson), &fields); err != nil {
		c.Infof("Error pulling fields from JSON: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	t := TuneResults{
		Data:      []byte(rawJson),
		Timestamp: time.Now(),
		Addr:      r.RemoteAddr,
		Country:   r.Header.Get("X-AppEngine-Country"),
		Region:    r.Header.Get("X-AppEngine-Region"),
		City:      r.Header.Get("X-AppEngine-City"),
		UUID:      fields.UUID,
		Board:     fields.Vehicle.Firmware.Board,
		Tau:       fields.Identification.Tau,
	}

	fmt.Sscanf(r.Header.Get("X-Appengine-Citylatlong"),
		"%f,%f", &t.Lat, &t.Lon)

	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(&t); err != nil {
		c.Infof("Error encoding tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	task := &taskqueue.Task{
		Path:    "/asyncStoreTune",
		Payload: buf.Bytes(),
	}
	if _, err := taskqueue.Add(c, task, "asyncstore"); err != nil {
		c.Infof("Error queueing storage of tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(201)
}

func handleAsyncStoreTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	var t TuneResults
	if err := gob.NewDecoder(r.Body).Decode(&t); err != nil {
		c.Errorf("Error decoding tune results: %v", err)
		http.Error(w, "error decoding gob", 500)
		return
	}

	oldSize := len(t.Data)
	if err := t.compress(); err != nil {
		c.Errorf("Error compressing raw tune data: %v", err)
		http.Error(w, "error compressing raw tune data", 500)
		return
	}
	c.Infof("Compressed stat data from %v -> %v", oldSize, len(t.Data))

	_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "TuneResults", nil), &t)
	if err != nil {
		c.Warningf("Error storing tune results item:  %v", err)
		http.Error(w, "error storing tune results", 500)
		return
	}

	w.WriteHeader(201)
}

func fetchVals(b []byte, cols []string) ([]string, error) {
	rv := make([]string, 0, len(cols))
	for _, k := range cols {
		var v interface{}
		if err := jsonpointer.FindDecode(b, k, &v); err != nil {
			return nil, fmt.Errorf("field %v: %v", k, err)
		}
		rv = append(rv, fmt.Sprint(v))
	}
	return rv, nil
}

func columnize(s []string) []string {
	rv := make([]string, 0, len(s))
	for _, k := range s {
		rv = append(rv, strings.Replace(k[1:], "/", ".", -1))
	}
	return rv
}

func handleExportTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	header := []string{"timestamp", "uuid", "country", "region", "city", "lat", "lon"}

	jsonCols := []string{
		"/vehicle/batteryCells", "/vehicle/esc",
		"/vehicle/motor", "/vehicle/size", "/vehicle/type",
		"/vehicle/weight",
		"/vehicle/firmware/board",
		"/vehicle/firmware/commit",
		"/vehicle/firmware/date",
		"/vehicle/firmware/tag",

		"/identification/tau",
		"/identification/pitch/bias",
		"/identification/pitch/gain",
		"/identification/pitch/noise",
		"/identification/roll/bias",
		"/identification/roll/gain",
		"/identification/roll/noise",

		"/tuning/parameters/damping",
		"/tuning/parameters/noiseSensitivity",

		"/tuning/computed/derivativeCutoff",
		"/tuning/computed/naturalFrequency",
		"/tuning/computed/gains/outer/kp",
		"/tuning/computed/gains/pitch/kp",
		"/tuning/computed/gains/pitch/ki",
		"/tuning/computed/gains/pitch/kd",
		"/tuning/computed/gains/roll/kp",
		"/tuning/computed/gains/roll/ki",
		"/tuning/computed/gains/roll/kd",

		"/userObservations",
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()
	cw.Write(append(header, columnize(jsonCols)...))

	q := datastore.NewQuery("TuneResults").
		Order("timestamp")

	for t := q.Run(c); ; {
		var x TuneResults
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		}
		if err := x.uncompress(); err != nil {
			c.Infof("Error decompressing: %v", err)
			continue
		}

		jsonVals, err := fetchVals(x.Data, jsonCols)
		if err != nil {
			c.Infof("Error extracting fields from %s: %v", x.Data, err)
			continue
		}

		cw.Write(append([]string{
			x.Timestamp.Format(time.RFC3339), x.UUID,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
			jsonVals...,
		))
	}

}
