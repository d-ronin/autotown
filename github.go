package autotown

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/dustin/httputil"
	"github.com/simonz05/util/syncutil"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/urlfetch"
)

const (
	tagURL      = "https://api.github.com/repos/d-ronin/dRonin/tags"
	branchesURL = "https://api.github.com/repos/d-ronin/dRonin/branches"
	pullURL     = "https://api.github.com/repos/d-ronin/dRonin/pulls"
	hashURL     = "https://api.github.com/repos/d-ronin/dRonin/commits/"
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

func fetchDecode(c context.Context, u string, ob interface{}) error {
	log.Infof(c, "Fetching %v", u)
	defer func(start time.Time) { log.Infof(c, "Fetched %v in %v", u, time.Since(start)) }(time.Now())

	h := urlfetch.Client(c)
	res, err := h.Get(u)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return httputil.HTTPErrorf(res, "Error grabbing %v: %S\n%B", u)
	}

	defer res.Body.Close()

	d := json.NewDecoder(res.Body)
	return d.Decode(ob)
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
