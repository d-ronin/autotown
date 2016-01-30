package autotown

import (
	"fmt"
	"os"

	"github.com/dustin/go-nma"
	"golang.org/x/net/context"
	"google.golang.org/appengine/delay"
	"google.golang.org/appengine/urlfetch"
)

var notify = delay.Func("notify", func(c context.Context, title, msg, url string) error {
	key := os.Getenv("NMA_KEY")
	if key == "" {
		return fmt.Errorf("no NMA_KEY defined")
	}

	cl := urlfetch.Client(c)
	n := nma.NewWithClient(key, cl)

	return n.Notify(&nma.Notification{
		Application: "autotown",
		Event:       title,
		Description: msg,
		URL:         "http://dronin-autotown.appspot.com/static/stats.html",
	})
})
