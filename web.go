package autotown

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/base64"
	"encoding/csv"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os/user"
	"reflect"
	"strconv"
	"strings"
	"time"

	"camlistore.org/pkg/syncutil"

	"golang.org/x/net/context"

	"github.com/dustin/go-jsonpointer"

	"math"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/cloud/storage"
)

var templates *template.Template

func init() {
	var err error
	templates, err = template.New("").ParseGlob("templates/*")
	if err != nil {
		panic("Couldn't parse templates: " + err.Error())
	}

	http.HandleFunc("/storeTune", handleStoreTune)
	http.HandleFunc("/asyncStoreTune", handleAsyncStoreTune)
	http.HandleFunc("/storeCrash", handleStoreCrash)
	http.HandleFunc("/usageStats", handleUsageStats)
	http.HandleFunc("/asyncUsageStats", handleAsyncUsageStats)
	http.HandleFunc("/exportTunes", handleExportTunes)

	http.HandleFunc("/api/currentuser", handleCurrentUser)
	http.HandleFunc("/api/usageStats", handleUsageStatsSummary)
	http.HandleFunc("/api/recentTunes", handleRecentTunes)
	http.HandleFunc("/api/relatedTunes", handleRelatedTunes)
	http.HandleFunc("/api/recentUsage", handleRecentUsage)
	http.HandleFunc("/api/gitLabels", handleGitLabels)
	http.HandleFunc("/api/tune", handleTune)
	http.HandleFunc("/api/recentCrashes", handleRecentCrashes)
	http.HandleFunc("/at/", handleAutotown)

	http.HandleFunc("/r/entity/", handleEntityRedirect)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/at/", http.StatusFound)
	})
}

func execTemplate(c context.Context, w io.Writer, name string, obj interface{}) error {
	err := templates.ExecuteTemplate(w, name, obj)

	if err != nil {
		log.Errorf(c, "Error executing template %v: %v", name, err)
		if wh, ok := w.(http.ResponseWriter); ok {
			http.Error(wh, "Error executing template", 500)
		}
	}
	return err
}

func handleStoreTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	rawJson := json.RawMessage{}
	if err := json.NewDecoder(r.Body).Decode(&rawJson); err != nil {
		log.Infof(c, "Error handling input JSON: %v", err)
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
		log.Infof(c, "Error pulling fields from JSON: %v", err)
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

	oldSize := len(t.Data)
	if err := t.compress(); err != nil {
		log.Errorf(c, "Error compressing raw tune data: %v", err)
		http.Error(w, "error compressing raw tune data", 500)
		return
	}
	log.Infof(c, "Compressed stat data from %v -> %v", oldSize, len(t.Data))

	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(&t); err != nil {
		log.Infof(c, "Error encoding tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	k, err := datastore.Put(c, datastore.NewIncompleteKey(c, "TuneResults", nil), &t)
	if err != nil {
		log.Infof(c, "Error performing initial put (queueing): %v", err)
		task := &taskqueue.Task{
			Path:    "/asyncStoreTune",
			Payload: buf.Bytes(),
		}
		if _, err := taskqueue.Add(c, task, "asyncstore"); err != nil {
			log.Infof(c, "Error queueing storage of tune results: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(202)
		return
	}

	w.Header().Set("Location", "https://dronin-autotown.appspot.com/at/tune/"+k.Encode())
	w.WriteHeader(201)
}

func handleAsyncStoreTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	var t TuneResults
	if err := gob.NewDecoder(r.Body).Decode(&t); err != nil {
		log.Errorf(c, "Error decoding tune results: %v", err)
		http.Error(w, "error decoding gob", 500)
		return
	}

	_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "TuneResults", nil), &t)
	if err != nil {
		log.Warningf(c, "Error storing tune results item:  %v", err)
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

func exportTunesCSV(c context.Context, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/csv")

	header := []string{"timestamp", "id", "country", "region", "city", "lat", "lon"}

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

	ids := map[string]string{}
	nextId := 1

	for t := q.Run(c); ; {
		var x TuneResults
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		}
		if err := x.uncompress(); err != nil {
			log.Infof(c, "Error decompressing: %v", err)
			continue
		}

		jsonVals, err := fetchVals(x.Data, jsonCols)
		if err != nil {
			log.Infof(c, "Error extracting fields from %s: %v", x.Data, err)
			continue
		}

		id, ok := ids[x.UUID]
		if !ok {
			id = strconv.Itoa(nextId)
			ids[x.UUID] = id
			nextId++
		}

		cw.Write(append([]string{
			x.Timestamp.Format(time.RFC3339), id,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
			jsonVals...,
		))
	}

}

func exportTunesJSON(c context.Context, w http.ResponseWriter, r *http.Request) {
	q := datastore.NewQuery("TuneResults").
		Order("timestamp")

	ids := map[string]string{}
	nextId := 1

	w.Header().Set("Content-Type", "application/json")
	j := json.NewEncoder(w)

	for t := q.Run(c); ; {
		type TuneResult struct {
			ID        string           `json:"id"`
			Timestamp time.Time        `json:"timestamp"`
			Addr      string           `json:"addr"`
			Country   string           `json:"country"`
			Region    string           `json:"region"`
			City      string           `json:"city"`
			Lat       float64          `json:"lat"`
			Lon       float64          `json:"lon"`
			TuneData  *json.RawMessage `json:"tuneData"`
		}
		var x TuneResults
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		}
		if err := x.uncompress(); err != nil {
			log.Infof(c, "Error decompressing: %v", err)
			continue
		}

		id, ok := ids[x.UUID]
		if !ok {
			id = strconv.Itoa(nextId)
			ids[x.UUID] = id
			nextId++
		}

		err = j.Encode(TuneResult{
			Timestamp: x.Timestamp,
			ID:        id,
			Addr:      x.Addr,
			Country:   x.Country,
			Region:    x.Region,
			City:      x.City,
			Lat:       x.Lat,
			Lon:       x.Lon,
			TuneData:  (*json.RawMessage)(&x.Data),
		})

		if err != nil {
			log.Infof(c, "Error writing entry: %v: %v", x, err)
		}
	}

}

func handleExportTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if r.FormValue("fmt") == "json" {
		exportTunesJSON(c, w, r)
		return
	}
	exportTunesCSV(c, w, r)
}

func handleStoreCrash(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	crash := &CrashData{}
	if err := json.NewDecoder(r.Body).Decode(&crash.properties); err != nil {
		log.Warningf(c, "Couldn't parse incoming JSON:  %v", err)
		http.Error(w, "Bad input: "+err.Error(), 400)
		return
	}

	data, err := base64.StdEncoding.DecodeString(crash.properties["dump"].(string))
	if err != nil {
		log.Warningf(c, "Couldn't parse decode crash:  %v", err)
		http.Error(w, "Bad input: "+err.Error(), 400)
		return
	}
	sum := sha1.Sum(data)
	filename := hex.EncodeToString(sum[:])
	filename = "crash/" + filename[:2] + "/" + filename[2:]
	delete(crash.properties, "dump")

	client, err := storage.NewClient(c)
	if err != nil {
		log.Warningf(c, "Error getting cloud store interface:  %v", err)
		http.Error(w, "error talking to cloud store", 500)
		return

	}
	defer client.Close()

	var bucketName string
	if bucketName, err = file.DefaultBucketName(c); err != nil {
		log.Errorf(c, "failed to get default GCS bucket name: %v", err)
		return
	}

	bucket := client.Bucket(bucketName)

	wc := bucket.Object(filename).NewWriter(c)
	wc.ContentType = "application/octet-stream"

	if _, err := wc.Write(data); err != nil {
		log.Warningf(c, "Error writing stuff to blob store:  %v", err)
		http.Error(w, "error writing to blob store", 500)
		return
	}
	if err := wc.Close(); err != nil {
		log.Warningf(c, "Error closing blob store:  %v", err)
		http.Error(w, "error closing blob store", 500)
		return
	}
	crash.properties["file"] = filename
	crash.properties["timestamp"] = time.Now()
	crash.properties["addr"] = r.RemoteAddr
	crash.properties["country"] = r.Header.Get("X-AppEngine-Country")
	crash.properties["region"] = r.Header.Get("X-AppEngine-Region")
	crash.properties["city"] = r.Header.Get("X-AppEngine-City")

	var lat, lon float64
	fmt.Sscanf(r.Header.Get("X-Appengine-Citylatlong"), "%f,%f", &lat, &lon)
	crash.properties["lat"] = lat
	crash.properties["lon"] = lon

	_, err = datastore.Put(c, datastore.NewIncompleteKey(c, "CrashData", nil), crash)
	if err != nil {
		log.Warningf(c, "Error storing tune results item:  %v\n%#v", err, crash)
		http.Error(w, "error storing tune results", 500)
		return
	}

	w.WriteHeader(204)
}

func handleAutotown(w http.ResponseWriter, r *http.Request) {
	execTemplate(appengine.NewContext(r), w, "app.html", nil)
}

func mustEncode(c context.Context, w io.Writer, i interface{}) {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}

	if err := json.NewEncoder(w).Encode(i); err != nil {
		log.Errorf(c, "Error json encoding: %v", err)
		if h, ok := w.(http.ResponseWriter); ok {
			http.Error(h, err.Error(), 500)
		}
		return
	}
}

func handleCurrentUser(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	u, _ := user.Current()
	mustEncode(c, w, u)
}

func fillKeyQuery(c context.Context, q *datastore.Query, results interface{}) error {
	keys, err := q.GetAll(c, results)
	if err == nil {
		rslice := reflect.ValueOf(results).Elem()
		for i := range keys {
			if k, ok := rslice.Index(i).Interface().(Keyable); ok {
				k.setKey(keys[i])
			} else if k, ok := rslice.Index(i).Addr().Interface().(Keyable); ok {
				k.setKey(keys[i])
			} else {
				// log.Infof(c, "Warning: %v is not Keyable", rslice.Index(i).Interface())
			}
		}
	} else {
		log.Errorf(c, "Error executing query: %v", err)
	}
	return err
}

func handleRecentTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := datastore.NewQuery("TuneResults").Order("-timestamp").Limit(50)
	res := []TuneResults{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	ids := map[string]string{}
	nextId := 1
	for i, x := range res {
		id, ok := ids[x.UUID]
		if !ok {
			id = strconv.Itoa(nextId)
			ids[x.UUID] = id
			nextId++
		}
		x.UUID = id

		res[i] = x
	}

	mustEncode(c, w, res)
}

func computeIceeTune(c context.Context, data []byte) map[string]float64 {
	var tune struct {
		Tuning struct {
			Computed struct {
				Gains struct {
					Pitch, Outer struct {
						KP, KI, KD float64
					}
				}
			}
		}
		Identification struct {
			Tau float64
		}
		RawSettings struct {
			SystemIdent struct {
				Fields struct {
					Beta []float64
				}
			}
		}
	}

	if err := json.Unmarshal(data, &tune); err != nil {
		log.Infof(c, "Error parsing data for experimental tunes: %v", err)
		return nil
	}

	if len(tune.RawSettings.SystemIdent.Fields.Beta) < 3 {
		log.Infof(c, "Error computing iceetune, not enough beta: %#v", tune)
		return nil
	}

	kp := tune.Tuning.Computed.Gains.Pitch.KP
	ki := tune.Tuning.Computed.Gains.Pitch.KI
	kd := tune.Tuning.Computed.Gains.Pitch.KD

	okp := tune.Tuning.Computed.Gains.Outer.KP
	tau := tune.Identification.Tau

	pbeta := tune.RawSettings.SystemIdent.Fields.Beta[1]
	ybeta := tune.RawSettings.SystemIdent.Fields.Beta[2]

	rv := map[string]float64{
		"yp":  kp * math.Pow(math.E, (pbeta-ybeta)*0.6),
		"yi":  ki * math.Pow(math.E, (pbeta-ybeta)*0.6) * 0.8,
		"yd":  kd * math.Pow(math.E, (pbeta-ybeta)*0.6) * 0.8,
		"oki": (1 / (2 * math.Pi * tau * 10.0) * 0.75) * okp,
	}

	return rv
}

func handleTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	k, err := datastore.DecodeKey(r.FormValue("tune"))
	if err != nil {
		log.Errorf(c, "Error parsing tune key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	tune := &TuneResults{}
	if err := datastore.Get(c, k, tune); err != nil {
		log.Errorf(c, "Error fetching tune: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	tune.Key = k

	if err := tune.uncompress(); err != nil {
		log.Errorf(c, "Error uncompressing tune details: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	tune.Orig = (*json.RawMessage)(&tune.Data)
	tune.Experimental = computeIceeTune(c, tune.Data)

	mustEncode(c, w, tune)
}

type relatedTune struct {
	Timestamp time.Time `datastore:"timestamp"`
	Addr      string    `datastore:"addr" json:"-"`
	Country   string    `datastore:"country"`
	Region    string    `datastore:"region"`
	City      string    `datastore:"city"`
	Lat       float64   `datastore:"lat"`
	Lon       float64   `datastore:"lon"`

	Orig *json.RawMessage

	Key *datastore.Key `datastore:"-"`
}

func (r *relatedTune) setKey(to *datastore.Key) {
	r.Key = to
}

func handleRelatedTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	k, err := datastore.DecodeKey(r.FormValue("tune"))
	if err != nil {
		log.Errorf(c, "Error parsing tune key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	tune := &TuneResults{}
	if err := datastore.Get(c, k, tune); err != nil {
		log.Errorf(c, "Error fetching tune: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	q := datastore.NewQuery("TuneResults").Filter("uuid = ", tune.UUID).
		Order("-timestamp").Limit(50)
	res := []TuneResults{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	for _, r := range res {
		if err := r.uncompress(); err != nil {
			log.Errorf(c, "Error uncompressing tune details: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		r.Orig = (*json.RawMessage)(&r.Data)
		r.UUID = ""
	}

	mustEncode(c, w, res)
}

func handleRecentCrashes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := datastore.NewQuery("CrashData").Order("-timestamp").Limit(50)
	res := []CrashData{}
	if err := fillKeyQuery(c, q, &res); err != nil {
		log.Errorf(c, "Error fetching crash results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	mustEncode(c, w, res)
}

type asyncUsageData struct {
	IP, Country, Region, City string
	Lat, Lon                  float64
	Timestamp                 time.Time
	RawData                   *json.RawMessage
}

func handleUsageStats(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	data := &asyncUsageData{
		IP:        r.RemoteAddr,
		Country:   r.Header.Get("X-AppEngine-Country"),
		Region:    r.Header.Get("X-AppEngine-Region"),
		City:      r.Header.Get("X-AppEngine-City"),
		Timestamp: time.Now(),
	}
	fmt.Sscanf(r.Header.Get("X-Appengine-Citylatlong"), "%f,%f", &data.Lat, &data.Lon)

	if err := json.NewDecoder(r.Body).Decode(&data.RawData); err != nil {
		log.Infof(c, "Error handling input JSON: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	j, err := json.Marshal(data)
	if err != nil {
		log.Infof(c, "Error marshaling input: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	g, err := gz(j)
	if err != nil {
		log.Infof(c, "Error compressing input: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	grp := syncutil.Group{}
	grp.Go(func() error {
		task := &taskqueue.Task{
			Path:    "/asyncUsageStats",
			Payload: g,
		}
		_, err := taskqueue.Add(c, task, "asyncusage")
		return err
	})
	grp.Go(func() error {
		task := &taskqueue.Task{
			Path:    "/asyncRollup",
			Payload: g,
		}
		_, err := taskqueue.Add(c, task, "asyncUsageRollup")
		return err
	})
	if err := grp.Err(); err != nil {
		log.Infof(c, "Error queueing storage of tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return

	}
	w.WriteHeader(202)
}

type recentUsage struct {
	City      string    `json:"city"`
	Region    string    `json:"region"`
	Country   string    `json:"country"`
	Lon       float64   `json:"lon"`
	Lat       float64   `json:"lat"`
	OS        string    `json:"os,omitempty"`
	Version   string    `json:"version,omitempty"`
	Boards    []string  `json:"boards,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

const maxRecent = 256

const usageRollupKey = "usageRollup"

func getRecent(c context.Context) ([]recentUsage, error) {
	recent := []recentUsage{}
	_, err := memcache.JSON.Get(c, usageRollupKey, &recent)
	return recent, err
}

func handleRecentUsage(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	recent, err := getRecent(c)
	if err != nil {
		log.Warningf(c, "Error getting recent: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	sincet, err := time.Parse(time.RFC3339, r.FormValue("since"))
	if err == nil {
		rv := []recentUsage{}
		for _, i := range recent {
			if i.Timestamp.After(sincet) {
				rv = append(rv, i)
			}
		}
		recent = rv
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	mustEncode(c, w, recent)
}

func handleAsyncUsageStats(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	recent := []recentUsage{}
	fetcherr := make(chan error, 1)
	go func() {
		_, err := memcache.JSON.Get(c, usageRollupKey, &recent)
		fetcherr <- err
	}()

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

	errch := make(chan error, 2)
	go func() {
		preSize := len(*d.RawData)

		u := UsageStat{
			Data:      []byte(*d.RawData),
			Timestamp: d.Timestamp,
			Addr:      d.IP,
			Country:   d.Country,
			Region:    d.Region,
			City:      d.City,
			Lat:       d.Lat,
			Lon:       d.Lon,
		}

		if err := u.compress(); err != nil {
			log.Errorf(c, "Error compressing: %v", err)
			errch <- err
			return
		}

		log.Infof(c, "Compressed usage data from %v to %v", preSize, len(u.Data))

		_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "UsageStat", nil), &u)
		if err != nil {
			log.Warningf(c, "Error storing usage data: %v", err)
			errch <- err
			return
		}
		errch <- nil
	}()

	go func() {
		decoded := struct {
			CurrentOS  string `json:"currentOS"`
			GCSVersion string `json:"gcs_version"`
			Boards     []struct {
				Name string
			} `json:"boards_seen"`
		}{}
		var boards []string
		if err := <-fetcherr; err != nil {
			log.Warningf(c, "Couldn't fetch recent values from memcached: %v", err)
		} else {
			if err := json.Unmarshal([]byte(*d.RawData), &decoded); err != nil {
				log.Warningf(c, "Error decoding usage details: %v", err)
			}
			m := map[string]bool{}
			for _, b := range decoded.Boards {
				m[b.Name] = true
			}
			for b := range m {
				boards = append(boards, b)
			}
		}
		recent = append(recent, recentUsage{
			Timestamp: d.Timestamp,

			Country: d.Country,
			Region:  d.Region,
			City:    d.City,
			Lat:     d.Lat,
			Lon:     d.Lon,
			OS:      decoded.CurrentOS,
			Version: decoded.GCSVersion,
			Boards:  boards,
		})
		if len(recent) > maxRecent {
			recent = recent[1:]
		}
		errch <- memcache.JSON.Set(c, &memcache.Item{
			Key:    usageRollupKey,
			Object: recent,
		})
	}()

	for i := 0; i < 2; i++ {
		if err := <-errch; err != nil {
			log.Warningf(c, "Error with storage stuff: %v", err)
		}
	}
}

func handleEntityRedirect(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	kstr := r.URL.Path[len("/r/entity/"):]

	k, err := datastore.DecodeKey(kstr)
	if err != nil {
		log.Errorf(c, "Error parsing tune key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	parts := []string{k.Namespace(), k.Kind(), "id:" + strconv.FormatInt(k.IntID(), 10)}
	for i := range parts {
		parts[i] = strconv.Itoa(len(parts[i])) + "/" + parts[i]
	}

	outk := url.QueryEscape(strings.Join(parts, "|"))

	http.Redirect(w, r, "https://console.developers.google.com/datastore/entities/edit?key="+
		outk+"&project="+appengine.AppID(c)+"&queryType=kind&kind="+k.Kind(), http.StatusFound)
}

const resultsStatsKey = "controllerStats"

func handleUsageStatsSummary(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	itm, err := memcache.Get(c, resultsStatsKey)
	if err == nil {
		rm := json.RawMessage(itm.Value)
		mustEncode(c, w, &rm)
		return
	}

	gitl, err := gitLabels(c)
	if err != nil {
		log.Errorf(c, "Error getting stuff from github, going without: %v", err)
	}

	results := struct {
		OSBoard      map[string]map[string]int `json:"os_board"`
		OSDetail     map[string]int            `json:"os_detail"`
		Board        map[string]int            `json:"board"`
		CountryBoard map[string]map[string]int `json:"country_board"`
		VersionBoard map[string]map[string]int `json:"version_board"`
	}{
		OSBoard:      map[string]map[string]int{},
		OSDetail:     map[string]int{},
		Board:        map[string]int{},
		CountryBoard: map[string]map[string]int{},
		VersionBoard: map[string]map[string]int{},
	}

	q := datastore.NewQuery("FoundController").Order("-timestamp")

	for t := q.Run(c); ; {
		var x FoundController
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		}

		results.OSDetail[x.GCSOS]++
		results.Board[x.Name]++

		cb, ok := results.CountryBoard[x.Country]
		if !ok {
			cb = map[string]int{}
		}
		cb[x.Name]++
		results.CountryBoard[x.Country] = cb

		ob, ok := results.OSBoard[abbrevOS(x.GCSOS)]
		if !ok {
			ob = map[string]int{}
		}
		ob[x.Name]++
		results.OSBoard[abbrevOS(x.GCSOS)] = ob

		ref := "Unknown"
		if lbls := gitDescribe(x.GitHash, gitl); lbls != nil {
			ref = lbls[0].Label
		}

		vb, ok := results.VersionBoard[ref]
		if !ok {
			vb = map[string]int{}
		}
		vb[x.Name]++
		results.VersionBoard[ref] = vb
	}

	memcache.JSON.Set(c, &memcache.Item{
		Key:        resultsStatsKey,
		Object:     results,
		Expiration: time.Hour,
	})

	mustEncode(c, w, results)
}
