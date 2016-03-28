package autotown

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/dustin/httputil"
	"go4.org/syncutil"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/urlfetch"
)

const (
	tagURL      = "https://api.github.com/repos/d-ronin/dRonin/tags"
	branchesURL = "https://api.github.com/repos/d-ronin/dRonin/branches"
	pullURL     = "https://api.github.com/repos/d-ronin/dRonin/pulls"
	hashURL     = "https://api.github.com/repos/d-ronin/dRonin/commits/"
	treeURL     = "https://api.github.com/repos/d-ronin/dRonin/git/trees/"
	blobURL     = "https://api.github.com/repos/d-ronin/dRonin/git/blobs/"

	maxConcurrent = 8
)

type githubTag struct {
	Name   string
	Commit struct {
		SHA string
	}
}

type githubPull struct {
	Title string
	URL   string
	Head  struct {
		Label string
		SHA   string
	}
}

type githubRef struct {
	Title string `json:"title"`
	Label string `json:"label"`
	Hash  string `json:"hash"`
	URL   string `json:"url"`
	Type  string `json:"type"`
}

type gitCommit struct {
	SHA    string
	Commit struct {
		Tree struct {
			SHA string
			URL string
		}
	}
}

type gitTreeEntry struct {
	Path string `datastore:"path,noindex"`
	Type string `datastore:"-"`
	SHA  string `datastore:"sha"`
	Size int64  `datastore:"size,noindex"`

	Root string `json:"-" datastore:"root"`
}

type gitTree struct {
	SHA       string
	URL       string
	Tree      []gitTreeEntry
	Truncated bool
}

type gitBlob struct {
	SHA  string `datastore:"sha"`
	Size int64  `datastore:"size,noindex"`
	Data []byte `datastore:"data" json:"content"`

	Filename string `json:"filename" datastore:"filename,noindex"`
}

func fetchDecode(c context.Context, u string, ob interface{}) error {
	log.Infof(c, "Fetching %v", u)
	defer func(start time.Time) { log.Infof(c, "Fetched %v in %v", u, time.Since(start)) }(time.Now())

	h := urlfetch.Client(c)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	if os.Getenv("GITHUB_USER") != "" {
		req.SetBasicAuth(os.Getenv("GITHUB_USER"), os.Getenv("GITHUB_AUTH_TOKEN"))
	}
	res, err := h.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		limit := res.Header.Get("X-RateLimit-Limit")
		remaining := res.Header.Get("X-RateLimit-Remaining")
		reset := res.Header.Get("X-RateLimit-Reset")
		return httputil.HTTPErrorf(res, "Error grabbing %v (limit=%v, remaining=%v, reset=%v): %S\n%B",
			u, limit, remaining, reset)
	}

	defer res.Body.Close()

	d := json.NewDecoder(res.Body)
	return d.Decode(ob)
}

func gzCacheSet(c context.Context, k string, age time.Duration, ob interface{}) error {
	j, err := json.Marshal(ob)
	if err == nil {
		b := &bytes.Buffer{}
		z, _ := gzip.NewWriterLevel(b, gzip.BestCompression)
		z.Write(j)
		z.Close()
		if err := memcache.Set(c, &memcache.Item{
			Key:        k,
			Value:      b.Bytes(),
			Expiration: age,
		}); err != nil {
			log.Infof(c, "Error setting cache: %v", err)
		}
	}
	return err
}

func gzCacheGet(c context.Context, k string, ob interface{}) error {
	it, err := memcache.Get(c, k)
	if err == nil {
		r, err := gzip.NewReader(bytes.NewReader(it.Value))
		if err == nil {
			if err := json.NewDecoder(r).Decode(ob); err == nil {
				log.Debugf(c, "%v was cached", k)
				return nil
			} else {
				log.Infof(c, "Error decoding from cache: %v", err)
			}
		} else {
			log.Infof(c, "Error ungzipping %d bytes from cache: %v", len(it.Value), err)
		}
	}
	return err
}

func fetchDecodeCached(c context.Context, k string, age time.Duration, u string, ob interface{}) error {
	if err := gzCacheGet(c, k, ob); err == nil {
		return nil
	}

	err := fetchDecode(c, u, ob)
	if err == nil {
		gzCacheSet(c, k, age, ob)
	}
	return err
}

func updateGithub(c context.Context) ([]githubRef, error) {
	log.Debugf(c, "Updating stuff from github")

	var tags, branches []githubTag
	var pulls []githubPull

	start := time.Now()
	g := syncutil.Group{}
	g.Go(func() error { return fetchDecode(c, tagURL, &tags) })
	g.Go(func() error { return fetchDecode(c, branchesURL, &branches) })
	g.Go(func() error { return fetchDecode(c, pullURL, &pulls) })

	if err := g.Err(); err != nil {
		return nil, err
	}

	log.Infof(c, "Finished fetching data from github in %v", time.Since(start))

	var rv []githubRef

	for _, t := range tags {
		rv = append(rv, githubRef{
			Type:  "tag",
			Label: t.Name,
			Title: t.Name,
			URL:   hashURL + t.Commit.SHA,
			Hash:  t.Commit.SHA,
		})
	}
	for _, t := range branches {
		rv = append(rv, githubRef{
			Type:  "branch",
			Title: t.Name,
			Label: t.Name,
			URL:   hashURL + t.Commit.SHA,
			Hash:  t.Commit.SHA,
		})
	}
	for _, p := range pulls {
		rv = append(rv, githubRef{
			Type:  "pull",
			Title: p.Title,
			Label: p.Head.Label,
			URL:   p.URL,
			Hash:  p.Head.SHA,
		})
	}

	log.Debugf(c, "Found %v references", len(rv))
	return rv, nil
}

const gitLabelKey = "githubLabels"

func gitLabels(c context.Context) ([]githubRef, error) {
	var rv []githubRef
	_, err := memcache.JSON.Get(c, gitLabelKey, &rv)
	if err != nil {
		log.Infof(c, "git labels not found in cache: %v", err)
		rv, err = updateGithub(c)
		if err != nil {
			return nil, err
		}
		memcache.JSON.Set(c, &memcache.Item{
			Key:        gitLabelKey,
			Object:     rv,
			Expiration: time.Hour,
		})
	}

	return rv, nil
}

func gitDescribe(h string, refs []githubRef) []githubRef {
	var rv []githubRef

	for _, r := range refs {
		if strings.HasPrefix(r.Hash, h) {
			rv = append(rv, r)
		}
	}

	return rv
}

func handleGitLabels(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	refs, err := gitLabels(c)
	if err != nil {
		log.Infof(c, "Error encoding tune results: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	mustEncode(c, w, refs)
}

func fetchBlob(c context.Context, g *syncutil.Group, h, filename string) (*gitBlob, error) {
	k := "blob@" + h
	blob := &gitBlob{}
	if err := gzCacheGet(c, k, blob); err == nil {
		return blob, nil
	}

	dk := datastore.NewKey(c, "GitBlob", h, 0, nil)
	if err := datastore.Get(c, dk, blob); err == nil {
		goto considered_harmful
	}

	if err := fetchDecode(c, blobURL+h, blob); err != nil {
		return nil, err
	}

	blob.Filename = filename
	g.Go(func() error {
		_, err := datastore.Put(c, dk, blob)
		if err != nil {
			log.Errorf(c, "Error storing blob: %v", err)
		}
		return nil
	})

	// Label, cache, and return
considered_harmful:
	blob.Filename = filename
	g.Go(func() error {
		gzCacheSet(c, k, 0, blob)
		return nil
	})
	return blob, nil
}

func fetchTree(c context.Context, g *syncutil.Group, h string) ([]gitTreeEntry, error) {
	k := "tree@" + h
	trees := []gitTreeEntry{}
	if err := gzCacheGet(c, k, &trees); err == nil {
		return trees, nil
	}

	q := datastore.NewQuery("GitTree").Filter("root =", h)
	for t := q.Run(c); ; {
		var x gitTreeEntry
		_, err := t.Next(&x)
		if err == datastore.Done {
			break
		} else if err != nil {
			return nil, err
		}

		trees = append(trees, x)
	}
	if len(trees) > 0 {
		gzCacheSet(c, k, 0, trees)
		return trees, nil
	}

	tree := &gitTree{}
	if err := fetchDecode(c, treeURL+h+"?recursive=1", tree); err != nil {
		return nil, err
	}
	if tree.Truncated {
		return nil, fmt.Errorf("Tree was truncated with %v items", len(tree.Tree))
	}

	var keys []*datastore.Key
	trees = nil
	for _, t := range tree.Tree {
		if !strings.HasPrefix(t.Path, "shared/uavobjectdefinition") {
			continue
		}
		if t.Type != "blob" {
			continue
		}
		t.Root = h
		trees = append(trees, t)
		keys = append(keys, datastore.NewIncompleteKey(c, "GitTree", nil))
	}

	g.Go(func() error {
		_, err := datastore.PutMulti(c, keys, trees)
		if err != nil {
			log.Errorf(c, "Error persisting tree state: %v", err)
		}
		return nil
	})

	g.Go(func() error { gzCacheSet(c, k, 0, trees); return nil })
	return trees, nil
}

func gitArchive(c context.Context, h string, w io.Writer) error {
	c, cancel := context.WithCancel(c)
	defer cancel()

	g := &syncutil.Group{}

	commit := &gitCommit{}
	if err := fetchDecodeCached(c, "commit@"+h, 0, hashURL+h, commit); err != nil {
		return err
	}

	blobs, err := fetchTree(c, g, commit.Commit.Tree.SHA)
	if err != nil {
		return err
	}

	gat := syncutil.NewGate(maxConcurrent)
	ch := make(chan *gitBlob)

	// This can be optimized considerably by bulk fetching all the
	// blob keys from memcache, and then any failing ones from
	// blob store before doing the individual work.  Similarly,
	// they can be bulk persisted and cached.
	// The downside is that convenience methods for decompressing
	// and decoding the cached data end up being specific to blobs.
	for _, t := range blobs {
		t := t
		g.Go(func() error {
			gat.Start()
			defer gat.Done()
			log.Debugf(c, "Fetching %v @ %v", t.Path, t.SHA)
			blob, err := fetchBlob(c, g, t.SHA, t.Path)
			if err != nil {
				cancel()
				return err
			}
			ch <- blob
			return nil
		})
	}

	go func() { g.Wait(); close(ch) }()

	gz := gzip.NewWriter(w)
	defer gz.Close()
	t := tar.NewWriter(gz)
	defer t.Close()

	for blob := range ch {
		err := t.WriteHeader(&tar.Header{
			Name: blob.Filename,
			Mode: 0644,
			Size: blob.Size,
		})
		if err != nil {
			return err
		}
		if _, err := t.Write(blob.Data); err != nil {
			return err
		}
	}

	return g.Err()
}

func handleUAVOs(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	w.Header().Set("Content-type", "application/tar+gzip")

	hashes := append([]string{r.URL.Path[7:]}, r.Form["altGitHash"]...)

	c := appengine.NewContext(r)

	for _, h := range hashes {
		k := "uavos@" + h
		it, err := memcache.Get(c, k)
		if err == nil {
			w.Write(it.Value)
			return
		}

		buf := &bytes.Buffer{}
		if err := gitArchive(c, h, buf); err != nil {
			log.Infof(c, "Error fetching stuff for %v: %v", h, err)
			continue
		}
		memcache.Set(c, &memcache.Item{
			Key:        k,
			Value:      buf.Bytes(),
			Expiration: time.Hour * 72,
		})

		w.Header().Set("X-Resolved-Ref", h)
		w.Write(buf.Bytes())
		return
	}

	http.Error(w, "No trees could be resolved.", 404)
}
