package autotown

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dustin/go-jsonpointer"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

const (
	mapStage1         = "map"
	mapStage2         = "map2"
	resubmitThreshold = 1000
	dayFmt            = "2006-01-02"
)

func init() {
	http.HandleFunc("/admin/batchForm", handleBatchForm)
	http.HandleFunc("/admin/submitMap", handleSubmitMap)

	http.HandleFunc("/batch/map", batchMap)
	http.HandleFunc("/batch/destroy", batchDestroy)

	http.HandleFunc("/batch/logkeys", handleLogKeys)
	http.HandleFunc("/batch/indexTunes", handleIndexTunes)
	http.HandleFunc("/batch/indexUsage", handleIndexUsage)
	http.HandleFunc("/batch/countUsage", handleCountUsage)

	http.HandleFunc("/batch/processUsage", handleProcessUsage)

	http.HandleFunc("/_ah/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	http.HandleFunc("/_ah/stop", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
}

func handleBatchForm(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	kinds, err := datastore.Kinds(c)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	execTemplate(appengine.NewContext(r), w, "batch.html", struct {
		Kinds   []string
		Message string
	}{
		kinds, r.FormValue("msg")})
}

func handleSubmitMap(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if r.FormValue("kind") == "" {
		http.Redirect(w, r, "/admin/batchForm?msg=Kind+parameter+is+required", http.StatusFound)
		return
	}

	_, err := taskqueue.Add(c, taskqueue.NewPOSTTask("/batch/map", r.Form), mapStage1)
	if err != nil {
		log.Errorf(c, "Error getting queue stats: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	if r.Header.Get("X-Appengine-Cron") == "true" {
		log.Infof(c, "Submitted on behalf of cron.")
		w.WriteHeader(204)
		return
	}

	http.Redirect(w, r, "/admin/batchForm?msg=Started", http.StatusFound)
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

func queueMore(c context.Context) bool {
	st, err := taskqueue.QueueStats(c, []string{mapStage2})
	if err != nil {
		log.Errorf(c, "Error getting queue stats: %v", err)
		return false
	}
	log.Debugf(c, "map2 queue stats: %+v", st[0])

	return st[0].Tasks < resubmitThreshold
}

func queueMany(c context.Context, queue string, tasks []*taskqueue.Task) error {
	g, _ := errgroup.WithContext(c)
	for len(tasks) > 0 {
		some := tasks
		if len(tasks) > 100 {
			some = tasks[:100]
			tasks = tasks[100:]
		} else {
			tasks = nil
		}

		g.Go(func() error {
			_, err := taskqueue.AddMulti(c, some, queue)
			return err
		})
	}
	return g.Wait()
}

// Params:
// - kind: The kind of thing to query
// - next: http path to process data
func batchMap(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	start := time.Now()

	if !queueMore(c) {
		log.Debugf(c, "Too many jobs queued, backing off")
		http.Error(w, "Busy", 503)
		return
	}

	q := datastore.NewQuery(r.FormValue("kind")).KeysOnly()
	if cstr := r.FormValue("cursor"); cstr != "" {
		cursor, err := datastore.DecodeCursor(cstr)
		maybePanic(err)
		log.Debugf(c, "Starting from cursor %v", cstr)
		q = q.Start(cursor)
	}

	keys := []string{}
	finished := false
	t := q.Run(c)
	for i := 0; i < 10000; i++ {
		k, err := t.Next(nil)
		if err == datastore.Done {
			finished = true
			break
		} else if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		keys = append(keys, k.Encode())
	}

	log.Infof(c, "Got %v %v keys in %v, finished=%v",
		len(keys), r.FormValue("kind"), time.Since(start), finished)

	var tasks []*taskqueue.Task
	for len(keys) > 0 && r.FormValue("next") != "" {
		subkeys := keys
		if len(subkeys) > 100 {
			subkeys = keys[:100]
			keys = keys[100:]
		} else {
			keys = nil
		}

		buf := &bytes.Buffer{}
		z := gzip.NewWriter(buf)
		e := json.NewEncoder(z)
		maybePanic(e.Encode(subkeys))
		maybePanic(z.Flush())
		maybePanic(z.Close())

		log.Debugf(c, "Queueing %v with %v keys compressed to %v bytes",
			mapStage2, len(subkeys), buf.Len())

		tasks = append(tasks, &taskqueue.Task{
			Path:    r.FormValue("next"),
			Payload: buf.Bytes(),
		})
	}

	if err := queueMany(c, mapStage2, tasks); err != nil {
		log.Errorf(c, "Error queueing task sets: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	if !finished {
		cursor, err := t.Cursor()
		maybePanic(err)

		log.Debugf(c, "Requesting more from %v", cursor.String())
		r.Form.Set("cursor", cursor.String())
		taskqueue.Add(c, taskqueue.NewPOSTTask("/batch/map", r.Form), mapStage1)
	}

	w.WriteHeader(201)
}

var destructionWhitelist = map[string]bool{"DailyCounts": true, "FoundController": true}

func batchDestroy(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keyStr := []string{}
	z, err := gzip.NewReader(r.Body)
	maybePanic(err)
	d := json.NewDecoder(z)
	maybePanic(d.Decode(&keyStr))
	log.Infof(c, "Got %v keys to destroy", len(keyStr))

	keys := []*datastore.Key{}
	for _, k := range keyStr {
		key, err := datastore.DecodeKey(k)
		if err != nil {
			log.Errorf(c, "Error decoding key: %v: %v", k, err)
			http.Error(w, err.Error(), 500)
			return
		}
		if !destructionWhitelist[key.Kind()] {
			log.Errorf(c, "Refusing to destroy non-whitelisted %v", key.Kind())
			http.Error(w, "Not whitelisted: "+key.Kind(), 400)
			return
		}
		keys = append(keys, key)
	}

	err = datastore.DeleteMulti(c, keys)
	if err != nil {
		log.Infof(c, "Error deleting things: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(204)
}

func decodeKeys(r io.Reader) ([]*datastore.Key, error) {
	z, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	keyStr := []string{}
	if err := json.NewDecoder(z).Decode(&keyStr); err != nil {
		return nil, err
	}

	keys := []*datastore.Key{}
	for _, k := range keyStr {
		key, err := datastore.DecodeKey(k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil
}

func handleLogKeys(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keys, err := decodeKeys(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	log.Debugf(c, "Got %v keys to process", len(keys))
	for _, k := range keys {
		log.Debugf(c, "%v", k)
	}
}

func jraw(c context.Context, m *json.RawMessage, path string) []byte {
	rv, err := jsonpointer.Find([]byte(*m), path)
	if err != nil {
		log.Warningf(c, "Error decoding %q from %v: %v", path, m, err)
	}
	return rv
}

func jptrs(c context.Context, m *json.RawMessage, path string) string {
	var rv string
	err := jsonpointer.FindDecode([]byte(*m), path, &rv)
	if err != nil {
		log.Warningf(c, "Error decoding %q from %s: %v", path, m, err)
	}
	return rv
}

func jptrf(c context.Context, m *json.RawMessage, path string) float64 {
	var rv interface{}
	err := jsonpointer.FindDecode([]byte(*m), path, &rv)
	if err != nil {
		log.Warningf(c, "Error decoding %q from %s: %v", path, m, err)
	}
	switch t := rv.(type) {
	case float64:
		return t
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			log.Warningf(c, "Error parsing string as float64: %v: %v", t, err)
		}
		return f
	default:
		log.Warningf(c, "Unexpected type for %q: %T", path, t)
	}
	return 0
}

func handleIndexTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keys, err := decodeKeys(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for _, k := range keys {
		tune, err := getTune(c, k)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err := tune.uncompress(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err := indexDoc(c, tune); err != nil {
			log.Errorf(c, "Error indexing: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
	}
}

func handleIndexUsage(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keys, err := decodeKeys(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for _, k := range keys {
		usage := &UsageStat{}
		if err := datastore.Get(c, k, usage); err != nil {
			log.Errorf(c, "Error fetching stat details: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		usage.Key = k

		if err := indexUsage(c, k.Encode(), usage); err != nil {
			log.Errorf(c, "Error indexing: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
	}
}

func countSomeUsage(c context.Context, fckeys []*datastore.Key) error {
	fcs := make([]*FoundController, len(fckeys))
	if err := datastore.GetMulti(c, fckeys, fcs); err != nil {
		return err
	}

	incrs := map[string]map[string]int64{}
	var fckup []*datastore.Key
	var fcup []*FoundController
	for i, fc := range fcs {
		if fc.Counted {
			continue
		}
		fckup = append(fckup, fckeys[i])
		fcup = append(fcup, fc)
		fc.Counted = true
		ds := fc.Oldest.Format(dayFmt)
		if _, ok := incrs[ds]; !ok {
			incrs[ds] = map[string]int64{}
		}

		incrs[ds][fc.Name]++
	}

	keys := []*datastore.Key{}
	for k := range incrs {
		keys = append(keys, datastore.NewKey(c, "DailyCounts", k, 0, nil))
	}
	if len(keys) == 0 {
		log.Debugf(c, "Nothing to do")
		return nil
	}

	counts := make([]DailyCounts, len(keys))
	err := datastore.GetMulti(c, keys, counts)
	if merr, ok := err.(appengine.MultiError); ok {
		for i, e := range merr {
			if e == datastore.ErrNoSuchEntity {
				counts[i].Counts = map[string]int64{}
			}
		}
		err = nil
	}
	if err != nil {
		return err
	}
	for i, e := range counts {
		i := incrs[keys[i].StringID()]
		for k, v := range i {
			e.Counts[k] += v
		}
	}
	if len(fckup)+len(keys) == 0 {
		log.Debugf(c, "Nothing to do")
		return nil
	}

	log.Infof(c, "Updating %v FoundControllers and %v DailyCounts",
		len(fckup), len(keys))

	if len(keys) > 0 {
		if _, err := datastore.PutMulti(c, keys, counts); err != nil {
			return err
		}
	}

	if len(fckup) > 0 {
		if _, err := datastore.PutMulti(c, fckup, fcup); err != nil {
			return err
		}
	}

	return nil
}

func handleCountUsage(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	fckeys, err := decodeKeys(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	grp, cc := errgroup.WithContext(c)
	for len(fckeys) > 0 {
		n := 10
		if n > len(fckeys) {
			n = len(fckeys)
		}
		todo := fckeys[:n]
		grp.Go(func() error {
			return datastore.RunInTransaction(cc, func(tc context.Context) error {
				return countSomeUsage(tc, todo)
			}, &datastore.TransactionOptions{XG: true, Attempts: 10})
		})
		fckeys = fckeys[n:]
	}

	if err := grp.Wait(); err != nil {
		log.Errorf(c, "Error counting usage: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(204)
}

func handleProcessUsage(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keys, err := decodeKeys(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	log.Debugf(c, "Got %v keys to process", len(keys))

	stats := make([]UsageStat, len(keys))
	err = datastore.GetMulti(c, keys, stats)
	if err != nil {
		log.Errorf(c, "Error grabbing all the stats from %v: %v", keys, err)
		http.Error(w, err.Error(), 500)
		return
	}

	grp, _ := errgroup.WithContext(c)
	var tasks []*taskqueue.Task
	total := 0
	for _, st := range stats {
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
				_, err := taskqueue.AddMulti(c, todo, "asyncRollupBE")
				return err
			})
			tasks = nil
			log.Debugf(c, "Added a batch of 100")
		}

		total++
	}

	if tasks != nil {
		grp.Go(func() error {
			_, err := taskqueue.AddMulti(c, tasks, "asyncRollupBE")
			return err
		})
		log.Debugf(c, "Added a batch of %v", len(tasks))
	}

	if err := grp.Wait(); err != nil {
		log.Errorf(c, "Error queueing stuff: %v", err)
		http.Error(w, "error queueing", 500)
		return
	}

	log.Debugf(c, "Queued %v entries for batch processing", total)

	w.WriteHeader(202)
}
