package autotown

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"crypto/sha256"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/admin/rewriteUUIDs", handleRewriteUUIDs)
	http.HandleFunc("/admin/updateControllers", handleUpdateControllers)
	http.HandleFunc("/admin/exportBoards", handleExportBoards)
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

	q := datastore.NewQuery("UsageStat")
	res := []UsageStat{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching usage stats results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	items := map[string]FoundController{}
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

		for _, b := range rec.BoardsSeen {
			uuid := b.UUID
			if uuid == "" {
				uuid = fmt.Sprintf("%x", sha256.Sum256([]byte(b.UUID)))
			}
			fc := items[uuid]
			if x.Timestamp.After(fc.Timestamp) {
				fc.UUID = uuid
				fc.Name = b.Name
				fc.GitHash = b.GitHash
				fc.GitTag = b.GitTag
				fc.UAVOHash = b.UavoHash
				fc.GCSOS = rec.CurrentOS
				fc.GCSArch = rec.CurrentArch
				fc.GCSVersion = rec.GCSVersion
				fc.Addr = x.Addr
				fc.Country = x.Country
				fc.Region = x.Region
				fc.City = x.City
				fc.Lat = x.Lat
				fc.Lon = x.Lon
				fc.Timestamp = x.Timestamp
				fc.Oldest = x.Timestamp
				if rec.ShareIP != "true" {
					fc.Addr = ""
				}
			}

			if x.Timestamp.Before(fc.Oldest) {
				fc.Oldest = x.Timestamp
			}

			fc.Count++

			items[uuid] = fc
		}
	}

	var keys []*datastore.Key
	var toUpdate []FoundController
	for k, v := range items {
		keys = append(keys, datastore.NewKey(c, "FoundController", k, 0, nil))
		toUpdate = append(toUpdate, v)
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

func abbrevOS(s string) string {
	switch {
	case strings.HasPrefix(s, "Windows"):
		return "Windows"
	case strings.HasPrefix(s, "Ubuntu"), strings.HasPrefix(s, "openSUSE"),
		strings.HasPrefix(s, "Gentoo"):
		return "Linux"
	case strings.HasPrefix(s, "OS X"):
		return "Mac"
	default:
		return s
	}
}

func handleExportBoards(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	w.Header().Set("Content-Type", "text/plain")

	header := []string{"timestamp", "oldest", "count",
		"uuid", "name", "git_hash", "git_tag", "uavo_hash",
		"gcs_os", "gcs_os_abbrev", "gcs_arch", "gcs_version",
		"country", "region", "city", "lat", "lon",
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()
	cw.Write(header)

	q := datastore.NewQuery("FoundController").Order("-timestamp")

	for t := q.Run(c); ; {
		var x FoundController
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		}

		cw.Write(append([]string{
			x.Timestamp.Format(time.RFC3339), x.Oldest.Format(time.RFC3339),
			fmt.Sprint(x.Count),
			x.UUID, x.Name, x.GitHash, x.GitTag, x.UAVOHash,
			x.GCSOS, abbrevOS(x.GCSOS), x.GCSArch, x.GCSVersion,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
		))
	}

}
