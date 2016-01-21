package autotown

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"crypto/sha256"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/admin/rewriteUUIDs", handleRewriteUUIDs)
	http.HandleFunc("/admin/updateControllers", handleUpdateControllers)
}

func handleRewriteUUIDs(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	q := datastore.NewQuery("TuneResults").Order("-timestamp").Limit(50)
	res := []TuneResults{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	var keys []*datastore.Key
	var toUpdate []TuneResults
	for _, x := range res {
		if len(x.UUID) == 64 {
			continue
		}
		prevuuid := x.UUID
		if err := x.uncompress(); err != nil {
			log.Errorf(c, "Error uncompressing %q: %v", x.UUID, err)
			continue
		}
		d := json.NewDecoder(bytes.NewReader(x.Data))
		d.UseNumber()
		m := map[string]interface{}{}
		err := d.Decode(&m)
		if err != nil {
			log.Errorf(c, "Error updating %q: %v", x.UUID, err)
			continue
		}
		x.UUID = fmt.Sprintf("%x", sha256.Sum256([]byte(x.UUID)))
		m["uniqueId"] = x.UUID
		x.Data, err = json.Marshal(m)
		if err != nil {
			log.Errorf(c, "Error encoding %q: %v", x.UUID, err)
			continue
		}
		if err := x.compress(); err != nil {
			log.Errorf(c, "Error compressing %q: %v", x.UUID, err)
			continue
		}
		log.Infof(c, "Updating %q -> %q for %v", prevuuid, x.UUID, x.Key.Encode())
		keys = append(keys, x.Key)
		toUpdate = append(toUpdate, x)
	}

	log.Infof(c, "Updating %v items", len(keys))
	_, err := datastore.PutMulti(c, keys, toUpdate)
	if err != nil {
		log.Errorf(c, "Error udpating tune records: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(204)
}

func handleUpdateControllers(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	q := datastore.NewQuery("UsageStat").Order("-timestamp").Limit(100)
	res := []UsageStat{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching usage stats results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	seen := map[string]time.Time{}
	var keys []*datastore.Key
	var toUpdate []FoundController
	for _, x := range res {
		x.uncompress()
		rec := struct {
			BoardsSeen []struct {
				CPU, UUID string
				FwHash    string
				GitHash   string
				GitTag    string
				Name      string
				UavoHash  string
			}
			CurrentArch, CurrentOS string
			GCSVersion             string `json:"gcs_version"`
			ShareIP                string
		}{}
		if err := json.Unmarshal(x.Data, &rec); err != nil {
			log.Warningf(c, "Couldn't parse %s: %v", x.Data, err)
			continue
		}
	board:
		for _, b := range rec.BoardsSeen {
			uuid := b.UUID
			if uuid == "" {
				uuid = fmt.Sprintf("%x", sha256.Sum256([]byte(b.UUID)))
			}
			if seen[uuid].After(x.Timestamp) {
				continue board
			}
			seen[uuid] = x.Timestamp
			k := datastore.NewKey(c, "FoundController", uuid, 0, nil)
			val := FoundController{
				UUID:       uuid,
				Name:       b.Name,
				GitHash:    b.GitHash,
				GitTag:     b.GitTag,
				UAVOHash:   b.UavoHash,
				GCSOS:      rec.CurrentOS,
				GCSArch:    rec.CurrentArch,
				GCSVersion: rec.GCSVersion,
				Addr:       x.Addr,
				Country:    x.Country,
				Region:     x.Region,
				City:       x.City,
				Lat:        x.Lat,
				Lon:        x.Lon,
				Timestamp:  x.Timestamp,
			}
			if rec.ShareIP != "true" {
				val.Addr = ""
			}
			keys = append(keys, k)
			toUpdate = append(toUpdate, val)
		}
	}

	log.Infof(c, "Updating %v items", len(keys))
	_, err := datastore.PutMulti(c, keys, toUpdate)
	if err != nil {
		log.Errorf(c, "Error updating controller records: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(204)

}
