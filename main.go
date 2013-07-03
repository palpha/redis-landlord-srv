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
	// "net/http"
	"github.com/garyburd/redigo/redis"
        "sync"
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

func listen() error {
  c, e := redis.Dial("tcp", ":6380")
  if e != nil {
    log.Printf("Unable to dial; %v", e)
    return e
  }
  defer c.Close()
  var wg sync.WaitGroup
  wg.Add(1)

  psc := redis.PubSubConn{c}

  go func() {
    //psc.Subscribe("setup")
    defer wg.Done()
    for {
      switch v := psc.Receive().(type) {
      case redis.Message:
        log.Printf("%s: message %s\n", v.Channel, v.Data)
      case redis.PMessage:
        log.Printf("PMessage: %s %s %s\n", v.Pattern, v.Channel, v.Data)
      case redis.Subscription:
        log.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
      case error:
        log.Printf("Receive fail; %v", e)
        return
      }
    }
  }()

  wg.Wait()
  return nil
}

var idValidator = regexp.MustCompile("^[_\\-a-zA-Z0-9]+$")

func main() {
        listen()

	log.Println("Exited")
	fmt.Println("Good bye.")
}
