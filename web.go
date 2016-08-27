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
	"math"
	"net/http"
	"net/url"
	"os/user"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/dustin/go-jsonpointer"
	"github.com/dustin/httputil"
	"github.com/rs/cors"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/urlfetch"
	"google.golang.org/cloud/storage"
)

const statsURL = "http://dronin-autotown.appspot.com/static/stats.html"

var (
	templates = template.Must(template.New("").ParseGlob("templates/*"))

	corsHandler = cors.New(cors.Options{
		AllowedOrigins: []string{"http://localhost:8080",
			"http://dronin.org", "https://dronin.org",
			"http://bl.ocks.org"},
		AllowedMethods: []string{"GET"},
	})
)

func init() {
	http.HandleFunc("/storeTune", handleStoreTune)
	http.HandleFunc("/asyncStoreTune", handleAsyncStoreTune)
	http.HandleFunc("/storeCrash", handleStoreCrash)
	http.HandleFunc("/storeTrace/", handleStoreTrace)
	http.HandleFunc("/usageStats", handleUsageStats)
	http.HandleFunc("/batch/asyncUsageStats", handleAsyncUsageStats)
	http.HandleFunc("/exportTunes", handleExportTunes)
	http.HandleFunc("/uavos/", handleUAVOs)

	http.HandleFunc("/api/currentuser", handleCurrentUser)
	http.Handle("/api/usageStats", corsHandleFunc(handleUsageStatsSummary))
	http.Handle("/api/usageDetails", corsHandleFunc(handleUsageStatsDetails))
	http.Handle("/api/recentTunes", corsHandleFunc(handleRecentTunes))
	http.Handle("/api/relatedTunes", corsHandleFunc(handleRelatedTunes))
	http.Handle("/api/recentUsage", corsHandleFunc(handleRecentUsage))
	http.Handle("/api/gitLabels", corsHandleFunc(handleGitLabels))
	http.Handle("/api/tune", corsHandleFunc(handleTune))
	http.Handle("/api/recentCrashes", corsHandleFunc(handleRecentCrashes))
	http.Handle("/api/crash/", corsHandleFunc(handleCrash))
	http.Handle("/api/crashtrace/", corsHandleFunc(handleTrace))
	http.HandleFunc("/at/", handleAutotown)

	http.HandleFunc("/r/entity/", handleEntityRedirect)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/at/", http.StatusFound)
	})
}

func corsHandleFunc(f func(w http.ResponseWriter, r *http.Request)) http.Handler {
	return corsHandler.Handler(http.HandlerFunc(f))
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
	log.Debugf(c, "Compressed stat data from %v -> %v", oldSize, len(t.Data))

	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(&t); err != nil {
		log.Infof(c, "Error encoding tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	grp, _ := errgroup.WithContext(c)

	k, err := datastore.Put(c, datastore.NewIncompleteKey(c, "TuneResults", nil), &t)
	if err != nil {
		log.Infof(c, "Error performing initial put (queueing): %v", err)
		task := &taskqueue.Task{
			Path:    "/asyncStoreTune",
			Payload: buf.Bytes(),
		}
		grp.Go(func() error {
			_, err := taskqueue.Add(c, task, "asyncstore")
			return err
		})

		if err := grp.Wait(); err != nil {
			log.Infof(c, "Error async processing tune: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(202)
		return
	}

	t.Key = k
	tuneURL := "https://dronin-autotown.appspot.com/at/tune/" + k.Encode()
	grp.Go(func() error { return cacheTune(c, &t) })

	if err := grp.Wait(); err != nil {
		log.Infof(c, "Error caching tune: %v", err)
	}

	log.Debugf(c, "Stored tune with key %v", k.Encode())

	w.Header().Set("Location", tuneURL)
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

	k, err := datastore.Put(c, datastore.NewIncompleteKey(c, "TuneResults", nil), &t)
	if err != nil {
		log.Warningf(c, "Error storing tune results item:  %v", err)
		http.Error(w, "error storing tune results", 500)
		return
	}

	log.Debugf(c, "Stored tune with key %v", k.Encode())

	t.Key = k
	if err := cacheTune(c, &t); err != nil {
		log.Warningf(c, "Error updating tune cache: %v", err)
	}

	if err := indexDoc(c, &t); err != nil {
		log.Warningf(c, "Error indexing tune: %v", err)
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

	header := []string{"timestamp", "key", "uuid", "country", "region", "city", "lat", "lon"}

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
		k, err := t.Next(&x)
		if err == datastore.Done {
			break
		} else if err != nil {
			panic(err)
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

		cw.Write(append([]string{
			x.Timestamp.Format(time.RFC3339), k.Encode(), x.UUID,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
			jsonVals...,
		))
	}

}

func exportTunesJSON(c context.Context, w http.ResponseWriter, r *http.Request) {
	q := datastore.NewQuery("TuneResults").
		Order("timestamp")

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
		} else if err != nil {
			panic(err)
		}
		if err := x.uncompress(); err != nil {
			log.Infof(c, "Error decompressing: %v", err)
			continue
		}

		err = j.Encode(TuneResult{
			Timestamp: x.Timestamp,
			ID:        x.UUID,
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

	k, err := datastore.Put(c, datastore.NewIncompleteKey(c, "CrashData", nil), crash)
	if err != nil {
		log.Warningf(c, "Error storing tune results item:  %v\n%#v", err, crash)
		http.Error(w, "error storing tune results", 500)
		return
	}
	crash.Key = k

	w.WriteHeader(204)

	// Header's done, just have to try to advise workers.
	notifyURL := "http://crash.dronin.tracer.nz/newCrash"
	j, err := json.Marshal(crash)
	if err != nil {
		log.Errorf(c, "Error serializing crash to JSON: %v", err)
		return
	}

	h := urlfetch.Client(c)
	req, err := http.NewRequest("POST", notifyURL, bytes.NewReader(j))
	if err != nil {
		log.Errorf(c, "Error creating notification request: %v", err)
		return
	}
	req.Header.Set("content-type", "application/json")
	res, err := h.Do(req)
	if err != nil {
		log.Errorf(c, "Error sending notification: %v", err)
		return
	}

	if res.StatusCode >= 300 || res.StatusCode < 200 {
		log.Errorf(c, "HTTP error delivering notification: %v", httputil.HTTPError(res))
	}

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

const recentTunesKey = "recentTunes"

func handleRecentTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	res := []TuneResults{}
	_, err := memcache.JSON.Get(c, recentTunesKey, &res)
	if err != nil {
		q := datastore.NewQuery("TuneResults").Order("-timestamp").Limit(500)
		if err := fillKeyQuery(c, q, &res); err != nil {
			log.Errorf(c, "Error fetching tune results: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		memcache.JSON.Set(c, &memcache.Item{
			Key:        recentTunesKey,
			Object:     res,
			Expiration: time.Hour * 24,
		})
	}

	rv := []*TuneResults{}
	seen := map[string]*TuneResults{}
	for _, t := range res {
		t.Board = canonicalBoard(t.Board)
		tune, ok := seen[t.UUID]
		if ok {
			tune.Older = append(tune.Older, timestampedTau{t.Tau, t.Timestamp, t.Key})
		} else {
			cp := t
			seen[t.UUID] = &cp
			rv = append(rv, &cp)
		}
	}

	if n, err := strconv.Atoi(r.FormValue("limit")); err == nil && n < len(rv) {
		rv = rv[:n]
	}

	mustEncode(c, w, rv)
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

	if tau < .005 {
		log.Infof(c, "Ignoring excessively low tau: %v", tune)
		return nil
	}

	pbeta := tune.RawSettings.SystemIdent.Fields.Beta[1]
	ybeta := tune.RawSettings.SystemIdent.Fields.Beta[2]

	if ybeta < 6.3 {
		log.Infof(c, "Error computing iceetune, yaw beta too low: %#v", ybeta)
		return nil
	}

	rv := map[string]float64{
		"yp":  kp * math.Pow(math.E, (pbeta-ybeta)*0.6),
		"yi":  ki * math.Pow(math.E, (pbeta-ybeta)*0.6) * 0.8,
		"yd":  kd * math.Pow(math.E, (pbeta-ybeta)*0.6) * 0.8,
		"oki": (1 / (2 * math.Pi * tau * 10.0) * 0.75) * okp,
	}

	return rv
}

func getTune(c context.Context, k *datastore.Key) (*TuneResults, error) {
	tunaKey := "/tune/" + k.String()

	tune := &TuneResults{}
	_, err := memcache.JSON.Get(c, tunaKey, tune)
	if err != nil {
		log.Infof(c, "Cache miss on %v, materializing", tunaKey)
		if err := datastore.Get(c, k, tune); err != nil {
			return nil, err
		}
		tune.Key = k

		if err := tune.uncompress(); err != nil {
			return nil, err
		}

		tune.Orig = (*json.RawMessage)(&tune.Data)
		tune.Experimental = computeIceeTune(c, tune.Data)

		memcache.JSON.Set(c, &memcache.Item{
			Key:    tunaKey,
			Object: tune,
		})
	}

	return tune, nil
}

func cacheTune(c context.Context, t *TuneResults) error {
	tunaKey := "/tune/" + t.Key.String()
	if err := t.uncompress(); err != nil {
		return err
	}
	t.Orig = (*json.RawMessage)(&t.Data)
	t.Experimental = computeIceeTune(c, t.Data)

	grp, _ := errgroup.WithContext(c)

	grp.Go(func() error {
		return memcache.JSON.Set(c, &memcache.Item{
			Key:    tunaKey,
			Object: t,
		})
	})

	grp.Go(func() error {
		memcache.DeleteMulti(c, []string{recentTunesKey, relatedKey(t.Key)})
		return nil
	})

	return grp.Wait()
}

func handleTune(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	k, err := datastore.DecodeKey(r.FormValue("tune"))
	if err != nil {
		log.Errorf(c, "Error parsing tune key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	tune, err := getTune(c, k)
	if err != nil {
		log.Errorf(c, "Error grabbing tune: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

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

func relatedKey(k *datastore.Key) string {
	return "/rtune/" + k.String()
}

func handleRelatedTunes(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	k, err := datastore.DecodeKey(r.FormValue("tune"))
	if err != nil {
		log.Errorf(c, "Error parsing tune key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	tune, err := getTune(c, k)
	if err != nil {
		log.Errorf(c, "Error fetching tune: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	tunaKey := relatedKey(k)
	res := []TuneResults{}
	_, err = memcache.JSON.Get(c, tunaKey, res)
	if err != nil {
		q := datastore.NewQuery("TuneResults").Filter("uuid = ", tune.UUID).
			Order("-timestamp").Limit(50)
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

		memcache.JSON.Set(c, &memcache.Item{
			Key:        tunaKey,
			Object:     res,
			Expiration: time.Hour * 24,
		})
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

func handleCrash(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	cid := r.URL.Path[len("/api/crash/"):]

	k, err := datastore.DecodeKey(cid)
	if err != nil {
		log.Errorf(c, "Error parsing crash key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	crash := &CrashData{}
	if err := datastore.Get(c, k, crash); err != nil {
		log.Errorf(c, "Error grabbing crash: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	crash.Key = k

	mustEncode(c, w, crash)
}

func handleTrace(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	cid := r.URL.Path[len("/api/crashtrace/"):]

	k, err := datastore.DecodeKey(cid)
	if err != nil {
		log.Errorf(c, "Error parsing crash key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	crash := &CrashData{}
	if err := datastore.Get(c, k, crash); err != nil {
		log.Errorf(c, "Error grabbing crash: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	fint, ok := crash.properties["trace"]
	if !ok {
		http.Error(w, "no trace found", 404)
		return
	}
	filename := fint.(string)

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
		http.Error(w, "error opening storage bucket", 500)
		return
	}

	bucket := client.Bucket(bucketName)
	rc, err := bucket.Object(filename).NewReader(c)
	if err != nil {
		log.Errorf(c, "Error opening %v: %v", filename, err)
		http.Error(w, "error opening trace file", 500)
		return
	}
	defer rc.Close()

	w.Header().Set("content-type", rc.ContentType())
	io.Copy(w, rc)
}

func handleStoreTrace(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	cid := r.URL.Path[len("/storeTrace/"):]

	k, err := datastore.DecodeKey(cid)
	if err != nil {
		log.Errorf(c, "Error parsing crash key: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}

	crash := &CrashData{}
	if err := datastore.Get(c, k, crash); err != nil {
		log.Errorf(c, "Error grabbing crash: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	crash.Key = k

	filename := crash.properties["file"].(string) + ".json"

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
		http.Error(w, "error opening storage bucket", 500)
		return
	}

	bucket := client.Bucket(bucketName)

	wc := bucket.Object(filename).NewWriter(c)
	wc.ContentType = "application/json"

	if _, err := io.Copy(wc, r.Body); err != nil {
		log.Warningf(c, "Error writing stuff to blob store:  %v", err)
		http.Error(w, "error writing to blob store", 500)
		return
	}
	if err := wc.Close(); err != nil {
		log.Warningf(c, "Error closing blob store:  %v", err)
		http.Error(w, "error closing blob store", 500)
		return
	}

	crash.properties["trace"] = filename

	_, err = datastore.Put(c, k, crash)
	if err != nil {
		log.Warningf(c, "Error storing crash trace:  %v\n%#v", err, crash)
		http.Error(w, "error storing crash trace", 500)
		return
	}

	log.Infof(c, "Stored crash in %v", k)

	w.WriteHeader(204)
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

	// https://groups.google.com/forum/?fromgroups#!topic/google-appengine/ik5fMyvO4PQ
	tid := traceId(r)
	log.Debugf(c, "setting traceid header to: %q", tid)
	task := &taskqueue.Task{
		Path:    "/batch/asyncUsageStats",
		Header:  http.Header{"X-Cloud-Trace-Context": []string{tid + "/0;o=1"}},
		Payload: g,
	}
	_, err = taskqueue.Add(c, task, "asyncusage")
	if err != nil {
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

	mustEncode(c, w, recent)
}

func traceId(r *http.Request) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %s %s", *r.URL, r.RemoteAddr, time.Now())
	return hex.EncodeToString(h.Sum(nil))[:32]
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

	g, _ := errgroup.WithContext(c)

	g.Go(func() error {
		return asyncRollup(c, &d)
	})

	g.Go(func() error {
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
			return err
		}

		log.Debugf(c, "Compressed usage data from %v to %v", preSize, len(u.Data))

		_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "UsageStat", nil), &u)
		if err != nil {
			log.Warningf(c, "Error storing usage data: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		decoded := struct {
			CurrentOS  string `json:"currentOS"`
			GCSVersion string `json:"gcs_version"`
			Boards     []struct {
				Name string
			} `json:"boardsSeen"`
		}{}
		var boards []string
		if err := <-fetcherr; err != nil {
			log.Infof(c, "Couldn't fetch recent values from memcached: %v", err)
		} else {
			if err := json.Unmarshal([]byte(*d.RawData), &decoded); err != nil {
				log.Warningf(c, "Error decoding usage details: %v", err)
			}
			m := map[string]bool{}
			for _, b := range decoded.Boards {
				m[canonicalBoard(b.Name)] = true
			}
			for b := range m {
				boards = append(boards, canonicalBoard(b))
			}
		}
		recent = append(recent, recentUsage{
			Timestamp: d.Timestamp,

			Country: d.Country,
			Region:  d.Region,
			City:    d.City,
			Lat:     d.Lat,
			Lon:     d.Lon,
			OS:      abbrevOS(decoded.CurrentOS),
			Version: decoded.GCSVersion,
			Boards:  boards,
		})
		if len(recent) > maxRecent {
			recent = recent[1:]
		}
		return memcache.JSON.Set(c, &memcache.Item{
			Key:    usageRollupKey,
			Object: recent,
		})
	})

	if err := g.Wait(); err != nil {
		log.Warningf(c, "Error with storage stuff: %v", err)
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

	var gitl []githubRef

	g, _ := errgroup.WithContext(c)
	g.Go(func() error {
		var err error
		gitl, err = gitLabels(c)
		if err != nil {
			log.Errorf(c, "Error getting stuff from github, going without: %v", err)
		}
		return nil
	})

	results := struct {
		OSBoard      map[string]map[string]int `json:"os_board"`
		OSDetail     map[string]int            `json:"os_detail"`
		Board        map[string]int            `json:"board"`
		BoardRev     map[string]map[string]int `json:"board_rev"`
		CountryBoard map[string]map[string]int `json:"country_board"`
		VersionBoard map[string]map[string]int `json:"version_board"`
	}{
		OSBoard:      map[string]map[string]int{},
		OSDetail:     map[string]int{},
		Board:        map[string]int{},
		BoardRev:     map[string]map[string]int{},
		CountryBoard: map[string]map[string]int{},
		VersionBoard: map[string]map[string]int{},
	}

	q := datastore.NewQuery("FoundController").Order("-timestamp")

	for t := q.Run(c); ; {
		var x FoundController
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		} else if err != nil {
			panic(err)
		}

		bn := canonicalBoard(x.Name)

		results.OSDetail[x.GCSOS]++
		results.Board[bn]++

		cb, ok := results.CountryBoard[x.Country]
		if !ok {
			cb = map[string]int{}
		}
		cb[bn]++
		results.CountryBoard[x.Country] = cb

		ob, ok := results.OSBoard[abbrevOS(x.GCSOS)]
		if !ok {
			ob = map[string]int{}
		}
		ob[bn]++
		results.OSBoard[abbrevOS(x.GCSOS)] = ob

		br, ok := results.BoardRev[bn]
		if !ok {
			br = map[string]int{}
		}
		br[fmt.Sprint(x.HardwareRev)]++
		results.BoardRev[bn] = br

		ref := "Unknown"
		g.Wait() // Make sure we have git labels
		if lbls := gitDescribe(x.GitHash, gitl); lbls != nil {
			ref = lbls[0].Label
		}

		vb, ok := results.VersionBoard[ref]
		if !ok {
			vb = map[string]int{}
		}
		vb[bn]++
		results.VersionBoard[ref] = vb
	}

	memcache.JSON.Set(c, &memcache.Item{
		Key:        resultsStatsKey,
		Object:     results,
		Expiration: time.Hour,
	})

	mustEncode(c, w, results)
}

func handleUsageStatsDetails(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	gitl, err := gitLabels(c)
	if err != nil {
		log.Warningf(c, "Couldn't resolve git labels: %v", err)
	}

	w.Header().Set("Content-Type", "text/plain")

	header := []string{"timestamp", "oldest", "count",
		"name", "hwev", "git_hash", "git_tag", "ref",
		"gcs_os", "gcs_os_abbrev", "gcs_version",
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
			canonicalBoard(x.Name), fmt.Sprint(x.HardwareRev), x.GitHash, x.GitTag, ref,
			x.GCSOS, abbrevOS(x.GCSOS), x.GCSVersion,
			x.Country, x.Region, x.City, fmt.Sprint(x.Lat), fmt.Sprint(x.Lon)},
		))
	}

}
