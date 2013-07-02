package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"path"
	"os/exec"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"github.com/garyburd/redigo/redis"
)

type Cfg struct {
	ManagerPath string
	ListenPort int
}

func readConfig() Cfg {
	file, e := ioutil.ReadFile("./config.json")
	if e != nil {
		log.Fatalf("Unable to read config.json: %s", e)
	}

	var cfg Cfg
	json.Unmarshal(file, &cfg)

	return cfg
}

var cfg Cfg

func setup(id string) (int, error) {
	c, e := redis.Dial("tcp", ":6380")
	if e != nil {
		return 0, e
	}

	if _, e := c.Do("AUTH", "landlord"); e != nil {
		c.Close()
		return 0, e
	}

	port, _ := redis.Int(c.Do("INCR", "port"))
	port = 6380 + port

	cmd := exec.Command("sudo", cfg.ManagerPath, "setup", id, strconv.Itoa(port))
	cmd.Dir = path.Dir(cfg.ManagerPath)
	output, e := cmd.CombinedOutput()
	if e != nil {
		log.Printf("Unable to run %s setup %s %d: %v\n", cfg.ManagerPath, id, port, e)
		log.Println(string(output[:]))

		if e.Error() != "exit status 7" {
			return 0, e
		}

		port, _ := redis.Int(c.Do("GET", fmt.Sprintf("port:%s", id)))
		if port == 0 {
			return 0, e
		}

		return port, nil
	}

	c.Do("SET", fmt.Sprintf("port:%s", id), port)
	log.Println(string(output[:]))

	return port, nil
}

func setupHandler(w http.ResponseWriter, r *http.Request, id string) {
	log.Printf("Setup request for %s", id)

	port, e := setup(id)
	if e != nil {
		http.Error(w, fmt.Sprintf("%v", e), 500)
	}

	fmt.Fprintf(w, "%d", port)
}

var idValidator = regexp.MustCompile("^[_\\-a-zA-Z0-9]+$")

func makeHandler(prefix string, fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len(prefix):]
		if !idValidator.MatchString(id) {
			http.Error(w, "Invalid id.", 500)
			return
		}

		fn(w, r, id)
	}
}

func main() {
	cfg = readConfig()
	http.HandleFunc("/setup/", makeHandler("/setup/", setupHandler))

	log.Printf("Listening on port %d\n", cfg.ListenPort)
	if e := http.ListenAndServe(fmt.Sprintf(":%d", cfg.ListenPort), nil); e != nil {
		log.Fatal(e)
	}

	log.Println("Exited")
	fmt.Println("Good bye.")
}
