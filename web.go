package statstore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"appengine"
	"appengine/datastore"
	_ "appengine/remote_api"
	"appengine/taskqueue"
)

var templates *template.Template

func init() {
	http.HandleFunc("/storeTune", handleStoreTune)
	http.HandleFunc("/asyncStoreTune", handleAsyncStoreTune)
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
				Date               time.Time
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
