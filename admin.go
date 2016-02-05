package autotown

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/simonz05/util/syncutil"

	"crypto/sha256"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
)

func init() {
	http.HandleFunc("/admin/rewriteUUIDs", handleRewriteUUIDs)
	http.HandleFunc("/admin/updateControllers", handleUpdateControllers)
	http.HandleFunc("/admin/exportBoards", handleExportBoards)
	http.HandleFunc("/batch/asyncRollup", handleAsyncRollup)
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

	q := datastore.NewQuery("UsageStat").Order("-timestamp")
	var tasks []*taskqueue.Task

	total := 0

	grp := syncutil.Group{}
	for t := q.Run(c); ; {
		var st UsageStat
		_, err := t.Next(&st)
		if err == datastore.Done {
			break
		} else if err != nil {
			panic(err)
		}

		err = st.uncompress()
		if err != nil {
			log.Warningf(c, "Failed to decompress record: %v", err)
			continue
		}

		rm := json.RawMessage(st.Data)
		data := &asyncUsageData{
			IP:        st.Addr,
			Country:   st.Country,
			Region:    st.Region,
			City:      st.City,
			Lat:       st.Lat,
			Lon:       st.Lon,
			Timestamp: st.Timestamp,
			RawData:   &rm,
		}

		j, err := json.Marshal(data)
		if err != nil {
			log.Infof(c, "Error marshaling input: %v", err)
			continue
		}

		g, err := gz(j)
		if err != nil {
			log.Infof(c, "Error compressing input: %v", err)
			continue
		}

		tasks = append(tasks, &taskqueue.Task{
			Path:    "/batch/asyncRollup",
			Payload: g,
		})

		if len(tasks) == 100 {
			todo := tasks
			grp.Go(func() error {
				_, err := taskqueue.AddMulti(c, todo, "asyncRollup")
				return err
			})
			tasks = nil
			log.Infof(c, "Added a batch of 100")
		}

		total++

	}

	if tasks != nil {
		grp.Go(func() error {
			_, err := taskqueue.AddMulti(c, tasks, "asyncRollup")
			return err
		})
		log.Infof(c, "Added a batch of %v", len(tasks))
	}

	if err := grp.Err(); err != nil {
		log.Errorf(c, "Error queueing stuff: %v", err)
		http.Error(w, "error queueing", 500)
		return
	}

	log.Infof(c, "Queued %v entries for batch processing", total)

	w.WriteHeader(204)
}

type usageSeenBoard struct {
	ID        int
	CPU, UUID string
	FwHash    string
	GitHash   string
	GitTag    string
	Name      string
	UavoHash  string
}

func handleAsyncRollup(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	var d asyncUsageData
	br, err := gzip.NewReader(r.Body)
	if err != nil {
		log.Errorf(c, "Error initializing ungzip: %v", err)
		http.Error(w, "error ungzipping", 500)
		return
	}
	if err := json.NewDecoder(br).Decode(&d); err != nil {
		log.Errorf(c, "Error decoding async json data: %v", err)
		http.Error(w, "error decoding json", 500)
		return
	}

	if err := asyncRollup(c, &d); err != nil {
		log.Errorf(c, "Error doing async rollup: %v", err)
		http.Error(w, "error doing async rollup", 500)
		return
	}

	w.WriteHeader(204)
}

func asyncRollup(c context.Context, d *asyncUsageData) error {
	rec := struct {
		BoardsSeen             []usageSeenBoard
		CurrentArch, CurrentOS string
		GCSVersion             string `json:"gcs_version"`
		ShareIP                string
	}{}
	if err := json.Unmarshal([]byte(*d.RawData), &rec); err != nil {
		log.Warningf(c, "Couldn't parse %s: %v", *d.RawData, err)
		return err
	}

	seenBoards := map[string]usageSeenBoard{}
	for _, b := range rec.BoardsSeen {
		uuid := b.UUID
		if uuid == "" {
			if b.CPU == "" {
				log.Infof(c, "No UUID or CPU ID found for %v", b)
				continue
			}
			uuid = fmt.Sprintf("%x", sha256.Sum256([]byte(b.CPU)))
		}

		if b.Name == "CopterControl" {
			b.Name = "CC3D"
		}
		seenBoards[uuid] = b
	}

	items := map[string]FoundController{}
	for _, b := range seenBoards {
		uuid := b.UUID
		if uuid == "" {
			if b.CPU == "" {
				log.Infof(c, "No UUID or CPU ID found for %v", b)
				continue
			}
			uuid = fmt.Sprintf("%x", sha256.Sum256([]byte(b.CPU)))
		}

		if b.Name == "CopterControl" {
			b.Name = "CC3D"
		}

		fc := items[uuid]
		if d.Timestamp.After(fc.Timestamp) {
			fc.UUID = uuid
			fc.HardwareRev = b.ID & 0xff
			fc.Name = b.Name
			fc.GitHash = b.GitHash
			fc.GitTag = b.GitTag
			fc.UAVOHash = b.UavoHash
			fc.GCSOS = rec.CurrentOS
			fc.GCSArch = rec.CurrentArch
			fc.GCSVersion = rec.GCSVersion
			fc.Addr = d.IP
			fc.Country = d.Country
			fc.Region = d.Region
			fc.City = d.City
			fc.Lat = d.Lat
			fc.Lon = d.Lon
			fc.Timestamp = d.Timestamp
			fc.Oldest = d.Timestamp
			if rec.ShareIP != "true" {
				fc.Addr = ""
			}
		}

		if d.Timestamp.Before(fc.Oldest) {
			fc.Oldest = d.Timestamp
		}

		fc.Count++

		items[uuid] = fc
	}

	newBoard := ""
	var keys []*datastore.Key
	var toUpdate []FoundController
	for k, v := range items {
		key := datastore.NewKey(c, "FoundController", k, 0, nil)
		prev := &FoundController{}
		err := datastore.Get(c, key, prev)
		switch err {
		case datastore.ErrNoSuchEntity:
			newBoard = v.Name
		case nil:
		default:
			return err
		}

		if prev.Oldest.Before(v.Oldest) {
			v.Oldest = prev.Oldest
		}
		if prev.Timestamp.After(v.Timestamp) {
			v.Oldest = prev.Timestamp
		}

		keys = append(keys, key)
		toUpdate = append(toUpdate, v)
	}

	g := syncutil.Group{}
	if len(keys) > 0 {
		g.Go(func() error {
			log.Infof(c, "Updating %v items", len(keys))
			_, err := datastore.PutMulti(c, keys, toUpdate)
			memcache.Delete(c, resultsStatsKey)
			return err
		})
	}

	if newBoard != "" {
		g.Go(func() error {
			if err := notify.Call(c, "New Device", newBoard, statsURL); err != nil {
				log.Infof(c, "Error notifying about new board: %v", err)
			}
			return nil
		})
	}

	return g.Err()
}

func abbrevOS(s string) string {
	switch {
	case strings.HasPrefix(s, "Windows"):
		return "Windows"
	case strings.HasPrefix(s, "Ubuntu"), strings.HasPrefix(s, "openSUSE"),
		strings.HasPrefix(s, "Gentoo"), strings.HasPrefix(s, "Arch"),
		strings.HasPrefix(s, "Debian"):
		return "Linux"
	case strings.HasPrefix(s, "OS X"):
		return "Mac"
	default:
		return s
	}
}

func handleExportBoards(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	gitl, err := gitLabels(c)
	if err != nil {
		log.Warningf(c, "Couldn't resolve git labels: %v", err)
	}

	w.Header().Set("Content-Type", "text/plain")

	header := []string{"timestamp", "oldest", "count",
		"uuid", "name", "hwrev", "git_hash", "git_tag", "ref", "uavo_hash",
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
		} else if err != nil {
			panic(err)
		}

		ref := ""
		if lbls := gitDescribe(x.GitHash, gitl); lbls != nil {
			ref = lbls[0].Label
		}

		cw.Write(append([]string{
			x.Timestamp.Format(time.RFC3339), x.Oldest.Format(time.RFC3339),
			fmt.Sprint(x.Count),
			x.UUID, x.Name, fmt.Sprint(x.HardwareRev), x.GitHash, x.GitTag, ref, x.UAVOHash,
			x.GCSOS, abbrevOS(x.GCSOS), x.GCSArch, x.GCSVersion,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
		))
	}

}
