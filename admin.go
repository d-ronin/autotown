package autotown

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"crypto/sha256"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/admin/rewriteUUIDs", handleRewriteUUIDs)
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
