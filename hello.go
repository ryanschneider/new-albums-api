package hello

import (
    "bytes"
    "fmt"
    "io"
    
    "crypto/md5"
    "net/http"
    
    "appengine"
    "appengine/urlfetch"
    "appengine/memcache"

    "github.com/bitly/go-simplejson"
)

func init() {
    http.HandleFunc("/", handler)
}

func parseInput(c appengine.Context, r *http.Request) (input []string, err error) {
    defer r.Body.Close()
    var buf bytes.Buffer
    buf.ReadFrom(r.Body)
    c.Debugf("POST body is %d", buf.Len())
    b := buf.Bytes()
    payload, err := simplejson.NewJson(b)
    if err != nil {
        return
    }

    /*
    dbg, err := payload.EncodePretty()
    if err != nil {
        return
    }
    c.Debugf("POST body is %v", string(dbg))
    */

    search, err := payload.Get("search").Array()
    if err != nil {
        return
    }    

    for _, value := range search {
        i, ok := value.(string)
        if ok {
            input = append(input, i)
        }
    }
    return
}

func getResults(c appengine.Context, requests []string) (results *simplejson.Json, err error) {
    type AsyncResult struct {
        request string
        response *simplejson.Json
        err error 
    }

    done := make(chan AsyncResult)
    results = simplejson.New()

    for _, r := range requests {
        c.Infof("Request: %v", r)
        go func(req string) {
            h := md5.New()
            io.WriteString(h, req)
            key := fmt.Sprintf("%x", h.Sum(nil))
            c.Debugf("Check memcached: %v as %v", req, key)
            
            if cache, err := memcache.Get(c, key); err == nil {
                c.Debugf("Found memcached: %v as %v", req, key)
                j, err := simplejson.NewJson(cache.Value)
                result := AsyncResult{req, j, err}
                done <- result    
                return
            }

            c.Debugf("Fetch: %v", req)
            client := urlfetch.Client(c) 
            resp, err := client.Get(req)
            var buf bytes.Buffer
            defer resp.Body.Close()
            buf.ReadFrom(resp.Body)
            b := buf.Bytes()

            //c.Debugf("Async: %v", string(b[:]))
            if err == nil {
                j, err := simplejson.NewJson(b)
                cache := &memcache.Item{
                    Key: key,
                    Value: b,
                }

                if err := memcache.Add(c, cache); err != nil {
                    c.Debugf("Memcache set err: %v", err.Error())
                }

                result := AsyncResult{req, j, err}
                done <- result
            } else {
                c.Errorf("ERROR: %v", err.Error())
            }
        }(r)
    }

    c.Infof("Waiting for %d asyncs", len(requests))
    for i := 0; i < len(requests); i++ {
        if result := <-done; result.err == nil {
            results.Set(result.request, result.response)
        } 
    }
    return
}

func handler(w http.ResponseWriter, r *http.Request) {
    
    c := appengine.NewContext(r)

    if r.Method != "POST" {
        http.Error(w, "POST only please", http.StatusInternalServerError)
        return
    }

    if requests, err := parseInput(c, r); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    } else {

        if results, err := getResults(c, requests); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        } else {
            b, err := results.EncodePretty()
            if err == nil {
                w.Header().Add("content-type", "application/json")
                fmt.Fprintf(w, string(b[:]))
                return
            } else {
                http.Error(w, err.Error(), http.StatusInternalServerError)
                return
            }
        }
        
    }
}
