package autotown

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	"encoding/base64"

	"github.com/dustin/httputil"
	"go4.org/syncutil"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/urlfetch"
)

const (
	tagURL      = "https://api.github.com/repos/d-ronin/dronindRonin/tags"
	branchesURL = "https://api.github.com/repos/d-ronin/dRonin/branches"
	pullURL     = "https://api.github.com/repos/d-ronin/dRonin/pulls"
	hashURL     = "https://api.github.com/repos/d-ronin/dRonin/commits/"

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

type gitTree struct {
	SHA  string
	URL  string
	Tree []struct {
		Path string
		Mode int64 `json:"mode,string"`
		Type string
		SHA  string
		Size int64
		URL  string
	}
	Truncated bool
}

type gitBlob struct {
	SHA      string
	Size     int
	Content  string
	Encoding string

	filename string
	mode     int64
	size     int64
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

func fetchDecodeCached(c context.Context, k, u string, ob interface{}) error {
	_, err := memcache.JSON.Get(c, k, ob)
	if err != nil {
		err = fetchDecode(c, u, ob)
		if err == nil {
			memcache.JSON.Set(c, &memcache.Item{
				Key:        k,
				Object:     ob,
				Expiration: time.Hour * 72,
			})
		}
	} else {
		log.Debugf(c, "%v was cached", k)
	}
	return err
}

func updateGithub(c context.Context) ([]githubRef, error) {
	log.Infof(c, "Updating stuff from github")

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

	log.Infof(c, "Found %v references", len(rv))
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

func gitArchive(c context.Context, h string, w io.Writer) error {
	c, cancel := context.WithCancel(c)
	defer cancel()

	commit := &gitCommit{}
	if err := fetchDecodeCached(c, "commit@"+h, hashURL+h, commit); err != nil {
		return err
	}

	tree := &gitTree{}
	if err := fetchDecodeCached(c, "tree@"+commit.Commit.Tree.SHA, commit.Commit.Tree.URL+"?recursive=true", tree); err != nil {
		return err
	}

	if tree.Truncated {
		return fmt.Errorf("Tree was truncated with %v items", len(tree.Tree))
	}

	g := syncutil.Group{}
	gat := syncutil.NewGate(maxConcurrent)
	ch := make(chan *gitBlob)

	for _, t := range tree.Tree {
		if !strings.HasPrefix(t.Path, "shared/uavobjectdefinition") {
			continue
		}
		if t.Type != "blob" {
			continue
		}
		t := t
		g.Go(func() error {
			gat.Start()
			defer gat.Done()
			log.Debugf(c, "Fetching %v @ %v", t.Path, t.SHA)
			blob := &gitBlob{}
			if err := fetchDecodeCached(c, "blob@"+t.SHA, t.URL, blob); err != nil {
				cancel()
				return err
			}
			blob.filename = t.Path
			blob.mode = t.Mode
			blob.size = t.Size
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
			Name: blob.filename,
			Mode: 0644,
			Size: blob.size,
		})
		if err != nil {
			return err
		}
		if blob.Encoding == "base64" {
			b, err := base64.StdEncoding.DecodeString(blob.Content)
			if err != nil {
				return err
			}
			if _, err := t.Write(b); err != nil {
				return err
			}
		}
	}

	return g.Err()
}

func handleUAVOs(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	w.Header().Set("Content-type", "application/tar+gzip")

	h := r.URL.Path[7:]
	if err := gitArchive(c, h, w); err != nil {
		log.Infof(c, "Error fetching stuff for %v: %v", h, err)
		http.Error(w, err.Error(), 404)
		return
	}
}
