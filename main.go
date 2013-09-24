package main

import (
	"bitbucket.org/kardianos/osext"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"io/ioutil"
	"log"
	"os"
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
	LogPath        string
}

type ManagerError struct {
	ExitCode int
	Message  string
}

func (e *ManagerError) Error() string {
	return (*e).Message
}

type Instruction struct {
	ReplyTo string
	Op      string
	Id      string
}

type PlainResponse struct {
	Id     string
	Status string
	Error  string
}

type SetupResponse struct {
	Id     string
	Status string
	Error  string
	Port   int
}

var cfg Cfg
var idRe = regexp.MustCompile(`^[_\-a-zA-Z0-9]+$`)
var errRe = regexp.MustCompile(`^exit status ([0-9]+)$`)
var managerErrors = map[int]string{
	1: "Invalid command.",
	2: "Invalid instance id.",
	3: "Invalid port.",
	4: "Sudo required.",
	5: "Already installed",
	6: "Landlord not installed correctly.",
	7: "Instance already exists.",
	8: "Instance not enabled."}

func readConfig() *Cfg {
	// note: log will not be configured to write to file yet,
	// so who knows who'll see the log output at this stage...

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

	if cfg.LogPath == "" {
		cfg.LogPath =
			func() string {
				f, e := osext.ExecutableFolder()
				if e != nil {
					log.Fatal("Unable to read executable path, add LogPath to config.json.", e)
				}

				return f + "srv.log"
			}()
	}

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
	// refreshOccupiedPorts(c)

	s := redis.NewScript(3, `
		redis.call("SDIFFSTORE", KEYS[3], KEYS[1], KEYS[2])
		redis.call("ZINTERSTORE", KEYS[3], 1, KEYS[3])
		local freePort = redis.call("ZRANGE", KEYS[3], 0, 0)[1]
		redis.call("SADD", KEYS[2], freePort)
		if freePort == nil then
			return 0
		else
			return freePort
		end
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

func getPort(c *redis.Conn, id string) int {
	log.Printf("Getting port for %s", id)
	r, e := redis.Int((*c).Do("GET", getKey("tenant", id, "port")))
	if e != nil {
		log.Panicf("Unable to get port for %s: %v", id, e)
	}

	return r
}

func executeManagerOp(op string, id string, args ...string) (string, error) {
	allArgs := make([]string, 0, len(args)+3)
	allArgs = append(allArgs, cfg.ManagerPath, op, id)
	allArgs = append(allArgs, args...)

	log.Printf("Running sudo %s", strings.Join(allArgs, " "))

	cmd := exec.Command("sudo", allArgs...)
	cmd.Dir = path.Dir(cfg.ManagerPath)
	output, e := cmd.CombinedOutput()
	if e != nil {
		log.Printf("Unable to run %s: %v", strings.Join(allArgs, " "), e)
	}

	log.Println(string(output))

	return string(output), e
}

func parseManagerError(err string) ManagerError {
	idMatch := errRe.FindStringSubmatch(err)
	errf := func(id int, msg string) ManagerError {
		return ManagerError{ExitCode: id, Message: msg}
	}

	if idMatch == nil {
		return errf(0, fmt.Sprintf("Unknown error (%s).", err))
	}

	id, e := strconv.Atoi(idMatch[1])
	if e != nil {
		return errf(0, fmt.Sprintf("Invalid exit status (%s)", idMatch[1]))
	}

	if msg, ok := managerErrors[id]; ok {
		return errf(id, msg)
	} else {
		return errf(id, fmt.Sprintf("Unknown exit status (%d)", id))
	}
}

func setupInstance(id string) (rport int, err error) {
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
	if _, e := executeManagerOp("setup", id, strconv.Itoa(port)); e != nil {
		err := parseManagerError(e.Error())
		log.Printf("parsed: %v", err)
		if err.ExitCode != 7 {
			if port == 0 {
				panic(errors.New("No free ports. Increase MaxTenants, or set up a new Landlord server."))
			}

			panic(err)
		}

		releasePort(c, port)
		port = getPort(c, id)
	}

	return port, nil
}

func deleteInstance(id string) error {
	c := dial()
	defer (*c).Close()

	if _, e := executeManagerOp("delete", id); e != nil {
		switch e.Error() {
		case "exit status 9":
			return errors.New("Instance does not exist.")
		}

		return e
	}

	return nil
}

func getExistingPort(id string) (rport int, err error) {
	c := dial()
	defer (*c).Close()

	var port int

	defer func() {
		if e := recover(); e != nil {
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

	log.Printf("Getting port for \"%s\"", id)

	port = getPort(c, id)
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
			var msg string
			switch v := e.(type) {
			case error:
				msg = v.Error()
			case string:
				msg = v
			default:
				msg = fmt.Sprintf("%v", v)
			}

			dispatchResponse(instr.ReplyTo, &PlainResponse{Status: "ERROR", Error: msg})
		}
	}()

	if idRe.MatchString(instr.Id) == false {
		log.Panicf("Invalid id.")
	}

	log.Printf("Op: %s", instr.Op)

	switch instr.Op {
	case "Setup":
		rsp := &SetupResponse{Status: "OK"}
		rsp.Id = instr.Id
		port, e := setupInstance(instr.Id)
		if e != nil {
			rsp.Status = "ERROR"
			rsp.Error = e.Error()
		} else {
			rsp.Port = port
		}

		dispatchResponse(instr.ReplyTo, rsp)

	case "Delete":
		rsp := &PlainResponse{Status: "OK"}
		rsp.Id = instr.Id
		if e := deleteInstance(instr.Id); e != nil {
			rsp.Status = "ERROR"
			rsp.Error = e.Error()
		}

		dispatchResponse(instr.ReplyTo, rsp)

	case "GetPort":
		rsp := &SetupResponse{Status: "OK"}
		rsp.Id = instr.Id
		port, e := getExistingPort(instr.Id)
		if e != nil {
			rsp.Status = "ERROR"
			rsp.Error = e.Error()
		} else {
			rsp.Port = port
		}

		dispatchResponse(instr.ReplyTo, rsp)

	default:
		log.Printf("Unknown op: %s", instr.Op)

		rsp := &PlainResponse{Status: "ERROR", Error: "Unknown operation."}
		dispatchResponse(instr.ReplyTo, rsp)

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
					go handleInstruction(instr)
				}

				// If we can't read the instruction, we don't know to whom we
				// should respond. This is a slight problem, which could be solved
				// by using a pattern subscription and embedding the ReplyTo in
				// the channel name.
			case error:
				log.Printf("Receive fail; %v", v)
				return
			}
		}
	}()

	wg.Wait()
	return
}

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

func initLogging() {
	fmt.Printf("Creating file %s", cfg.LogPath)
	logf, e := os.OpenFile(cfg.LogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if e != nil {
		panic(errors.New(fmt.Sprintf("Unable to open log file %s: %v", cfg.LogPath, e)))
	}

	log.SetOutput(io.MultiWriter(logf, os.Stdout))
}

func main() {
	cfg = *readConfig()
	initLogging()
	log.Printf("Config: %v", cfg)
	prepareDb()
	listen()

	log.Println("Exited")
	fmt.Println("Good bye.")
}
