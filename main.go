package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"sync"
)

type Cfg struct {
	ManagerPath  string
	ListenPort   int
	LandlordPort int
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

	if cfg.ListenPort == 0 {
		cfg.ListenPort = 8080
	}

	if cfg.LandlordPort == 0 {
		cfg.LandlordPort = 6380
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

func setup(id string) (int, error) {
	c := *dial()
	defer c.Close()

	log.Printf("Setting up \"%s\"", id)

	port, _ := redis.Int(c.Do("INCR", "port"))
	port = 6380 + port

	cmd := exec.Command("sudo", cfg.ManagerPath, "setup", id, strconv.Itoa(port))
	cmd.Dir = path.Dir(cfg.ManagerPath)
	output, e := cmd.CombinedOutput()
	if e != nil {
		log.Printf("Unable to run %s setup %s %d: %v", cfg.ManagerPath, id, port, e)
		log.Println(string(output))

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
	log.Println(string(output))

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

var idValidator = regexp.MustCompile("^[_\\-a-zA-Z0-9]+$")

func main() {
	cfg = *readConfig()
	listen()

	log.Println("Exited")
	fmt.Println("Good bye.")
}
