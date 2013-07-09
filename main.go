package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type Cfg struct {
	ManagerPath    string
	ListenPort     int
	LandlordPort   int
	TenantPortBase int
	MaxTenants     int
}

type Instruction struct {
	ReplyTo string
	Op      string
	Id      string
}

type SetupResponse struct {
	Id     string
	Status string
	Error  string
	Port   int
}

var cfg Cfg

func readConfig() *Cfg {
	file, e := ioutil.ReadFile("./config.json")
	if e != nil {
		log.Fatalf("Unable to read config.json: %s", e)
	}

	var cfg Cfg
	json.Unmarshal(file, &cfg)

	if cfg.ManagerPath == "" {
		log.Fatalf("Strange config: %v", cfg)
	}

	if cfg.ListenPort <= 0 {
		cfg.ListenPort = 8080
	}

	if cfg.LandlordPort <= 0 {
		cfg.LandlordPort = 6380
	}

	if cfg.TenantPortBase <= 0 {
		cfg.TenantPortBase = 6381
	}

	if cfg.MaxTenants <= 0 {
		cfg.MaxTenants = 10
	}

	log.Printf("Config: %v", cfg)

	return &cfg
}

func toJson(obj interface{}) *[]byte {
	r, e := json.Marshal(obj)
	if e != nil {
		log.Panicf("Unable to marshal %v: %v", obj, e)
	}

	return &r
}

func dial() *redis.Conn {
	c, e := redis.Dial("tcp", ":"+strconv.Itoa(cfg.LandlordPort))
	if e != nil {
		log.Panicf("Dialling error: %v", e)
	}

	if _, e := c.Do("AUTH", "landlord"); e != nil {
		log.Panicf("AUTH error: %v", e)
	}

	return &c
}

func getKey(parts ...string) string {
	return "landlord:" + strings.Join(parts, ":")
}

func refreshOccupiedPorts(c *redis.Conn) {
	s := redis.NewScript(2, `
		local tenants = redis.call("SMEMBERS", KEYS[1])
		redis.call("DEL", KEYS[2])
		for i = 1, #tenants do
			local port = redis.call("GET", ARGV[1] .. ":" .. tenants[i] .. ":port")
			redis.call("SADD", KEYS[2], port)
		end
	`)

	if _, e := s.Do(*c,
		getKey("tenants"),
		getKey("ports", "occupied"),
		getKey("tenant")); e != nil {
		log.Panic("Unable to refresh occupied ports", e)
	}
}

func getFreePort(c *redis.Conn) int {
	refreshOccupiedPorts(c)

	s := redis.NewScript(3, `
		redis.call("SDIFFSTORE", KEYS[3], KEYS[1], KEYS[2])
		redis.call("ZINTERSTORE", KEYS[3], 1, KEYS[3])
		local freePort = redis.call("ZRANGE", KEYS[3], 0, 0)[1]
		redis.call("SADD", KEYS[2], freePort)
		return freePort
	`)

	r, e := redis.Int(s.Do(*c,
		getKey("ports", "possible"),
		getKey("ports", "occupied"),
		getKey("ports", "available")))
	if e != nil {
		log.Panic("Unable to fetch a free port: ", e)
	}

	return r
}

func releasePort(c *redis.Conn, port int) {
	if port <= 0 {
		return
	}

	log.Printf("Releasing port %d", port)
	if _, e := (*c).Do("SREM", getKey("ports", "occupied"), port); e != nil {
		log.Panicf("Unable to release port %d: %v", port, e)
	}
}

func persistPort(c *redis.Conn, id string, port int) {
	log.Printf("Persisting port %d for %s", port, id)
	f := func() error {
		if _, e := (*c).Do("SET", getKey("tenant", id, "port"), port); e != nil {
			return e
		}
		if _, e := (*c).Do("SADD", getKey("tenants"), id); e != nil {
			return e
		}
		return nil
	}

	if e := f(); e != nil {
		log.Panicf("Unable to persist port %d for %s: %v", port, id, e)
	}
}

func getPort(c *redis.Conn, id string) int {
	log.Printf("Getting port for %s", id)
	r, e := redis.Int((*c).Do("GET", getKey("tenant", id, "port")))
	if e != nil {
		log.Panicf("Unable to get port for %s: %v", id, e)
	}

	return r
}

func setup(id string) (rport int, err error) {
	c := dial()
	defer (*c).Close()

	var port int

	log.Printf("Setting up \"%s\"", id)

	defer func() {
		if e := recover(); e != nil {
			releasePort(c, port)
			rport = 0
			switch v := e.(type) {
			case error:
				err = v
			case string:
				err = errors.New(v)
			default:
				err = fmt.Errorf("%s", v)
			}
		}
	}()

	port = getFreePort(c)

	log.Printf("Running sudo %s setup %s %i", cfg.ManagerPath, id, port)
	cmd := exec.Command("sudo", cfg.ManagerPath, "setup", id, strconv.Itoa(port))
	cmd.Dir = path.Dir(cfg.ManagerPath)
	output, e := cmd.CombinedOutput()
	if e != nil {
		log.Printf("Unable to run %s setup %s %d: %v", cfg.ManagerPath, id, port, e)
		log.Println(string(output))

		if e.Error() != "exit status 7" {
			panic(e)
		}

		releasePort(c, port)
		port = getPort(c, id)
	}

	log.Println(string(output))
	persistPort(c, id, port)

	return port, nil
}

func dispatchResponse(recipient string, rsp interface{}) {
	c := *dial()
	defer c.Close()

	log.Printf("Responding to %s: %v", recipient, rsp)
	payload := toJson(rsp)
	c.Do("PUBLISH", "landlord.response."+recipient, string(*payload))
}

func readInstruction(msg *redis.Message) *Instruction {
	var instr Instruction
	if e := json.Unmarshal(msg.Data, &instr); e != nil {
		log.Printf("Unable to read instruction \"%s\"; %v", msg.Data, e)
		return nil
	}

	return &instr
}

func handleInstruction(instr *Instruction) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("Panic stopped: %v", e)
		}
	}()

	switch instr.Op {
	case "Setup":
		rsp := new(SetupResponse)
		rsp.Id = instr.Id
		port, e := setup(instr.Id)
		if e != nil {
			rsp.Status = "ERROR"
			rsp.Error = e.Error()
		} else {
			rsp.Status = "OK"
			rsp.Port = port
		}

		dispatchResponse(instr.ReplyTo, rsp)
	default:
		log.Printf("Unknown op: %s", instr.Op)
	}
}

func listen() {
	c := *dial()
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	psc := redis.PubSubConn{c}

	go func() {
		psc.Subscribe("landlord.request")
		defer wg.Done()
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				log.Printf("%s: message %s", v.Channel, v.Data)
				if instr := readInstruction(&v); instr != nil {
					handleInstruction(instr)
				}
			case error:
				log.Printf("Receive fail; %v", v)
				return
			}
		}
	}()

	wg.Wait()
	return
}

var idValidator = regexp.MustCompile(`^[_\-a-zA-Z0-9]+$`)

func prepareDb() {
	c := dial()
	defer (*c).Close()

	s := redis.NewScript(1, `
	  redis.call("DEL", KEYS[1])
	  local portBase = tonumber(ARGV[1])
		local maxPorts = tonumber(ARGV[2])
		for i = 0, maxPorts - 1 do
		  redis.call("SADD", KEYS[1], portBase + i)
		end
	`)

	if _, e := s.Do(*c,
		getKey("ports", "possible"),
		cfg.TenantPortBase,
		cfg.MaxTenants); e != nil {
		log.Panicf("Unable to prepare database: %v", e)
	}

	refreshOccupiedPorts(c)
}

func main() {
	cfg = *readConfig()
	prepareDb()
	listen()

	log.Println("Exited")
	fmt.Println("Good bye.")
}
