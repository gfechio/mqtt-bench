package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const BASE_TOPIC string = "/mqtt-bench/benchmark"

var Debug bool = false

// Uses the default handler when subscribing
var DefaultHandlerResults []*SubscribeResult

// Allows the processing result of DefaultHandler at the time of Subscribe to be retained for Apollo
type ExecOptions struct {
	Broker            string     // Broker URI
	Qos               byte       // QoS(0|1|2)
	Retain            bool       // Retain
	Topic             string     // Topic Route
	Username          string     // UserID
	Password          string     // User Password
	CertConfig        CertConfig // Authentication Definition
	ClientNum         int        // Number of concurrent clients
	Count             int        // Number of messages per client
	MessageSize       int        // Message size (byte)
	UseDefaultHandler bool       // Whether to use the default MessageHandler instead of the Subscriber individually
	PreTime           int        // Wait time before execution(ms)
	IntervalTime      int        // Execution interval time for each message(ms)
}

// Authentication settings
type CertConfig interface{}

//Server authentication settings
type ServerCertConfig struct {
	CertConfig
	ServerCertFile string // サーバ証明書ファイル
}

// Client authentication settings
type ClientCertConfig struct {
	CertConfig
	RootCAFile     string // ルート証明書ファイル
	ClientCertFile string // クライアント証明書ファイル
	ClientKeyFile  string // クライアント公開鍵ファイル
}

//Generate a TLS configuration for the server certificate.
//   serverCertFile : Server certificate file
func CreateServerTlsConfig(serverCertFile string) *tls.Config {
	certpool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(serverCertFile)
	if err == nil {
		certpool.AppendCertsFromPEM(pem)
	}

	return &tls.Config{
		RootCAs: certpool,
	}
}

// Generate a TLS configuration for the client certificate.
//   rootCAFile     : Root certificate file
//   clientCertFile : Client certificate file
//   clientKeyFile  : Client public key file
func CreateClientTlsConfig(rootCAFile string, clientCertFile string, clientKeyFile string) *tls.Config {
	certpool := x509.NewCertPool()
	rootCA, err := ioutil.ReadFile(rootCAFile)
	if err == nil {
		certpool.AppendCertsFromPEM(rootCA)
	}

	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		panic(err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
}

// Execute Function
func Execute(exec func(clients []*MQTT.Client, opts ExecOptions, param ...string) int, opts ExecOptions) {
	message := CreateFixedSizeMessage(opts.MessageSize)

	// Initialize the array
	DefaultHandlerResults = make([]*SubscribeResult, opts.ClientNum)

	clients := make([]*MQTT.Client, opts.ClientNum)
	hasErr := false
	for i := 0; i < opts.ClientNum; i++ {
		client := Connect(i, opts)
		if client == nil {
			hasErr = true
			break
		}
		clients[i] = client
	}

	// If there is a connection error, disconnect the connected client and end the process.
	if hasErr {
		for i := 0; i < len(clients); i++ {
			client := clients[i]
			if client != nil {
				Disconnect(client)
			}
		}
		return
	}

	// Wait for a certain period of time to stabilize.
	time.Sleep(time.Duration(opts.PreTime) * time.Millisecond)

	fmt.Printf("%s Start benchmark\n", time.Now())

	startTime := time.Now()
	totalCount := exec(clients, opts, message)
	endTime := time.Now()

	fmt.Printf("%s End benchmark\n", time.Now())

	// Since it takes time to disconnect, processing is performed asynchronously.
	AsyncDisconnect(clients)

	// Output the processing result.
	duration := (endTime.Sub(startTime)).Nanoseconds() / int64(1000000) // nanosecond -> millisecond
	throughput := float64(totalCount) / float64(duration) * 1000        // messages/sec
	fmt.Printf("\nResult : broker=%s, clients=%d, totalCount=%d, duration=%dms, throughput=%.2fmessages/sec\n",
		opts.Broker, opts.ClientNum, totalCount, duration, throughput)
}

// Perform publish processing for all clients.
// Returns the number of messages sent (in principle, the number of clients).
func PublishAllClient(clients []*MQTT.Client, opts ExecOptions, param ...string) int {
	message := param[0]

	wg := new(sync.WaitGroup)

	totalCount := 0
	for id := 0; id < len(clients); id++ {
		wg.Add(1)

		client := clients[id]

		go func(clientId int) {
			defer wg.Done()

			for index := 0; index < opts.Count; index++ {
				topic := fmt.Sprintf(opts.Topic+"/%d", clientId)

				if Debug {
					fmt.Printf("Publish : id=%d, count=%d, topic=%s\n", clientId, index, topic)
				}
				Publish(client, topic, opts.Qos, opts.Retain, message)
				totalCount++

				if opts.IntervalTime > 0 {
					time.Sleep(time.Duration(opts.IntervalTime) * time.Millisecond)
				}
			}
		}(id)
	}

	wg.Wait()

	return totalCount
}

// Send a message.
func Publish(client *MQTT.Client, topic string, qos byte, retain bool, message string) {
	token := client.Publish(topic, qos, retain, message)

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Publish error: %s\n", token.Error())
	}
}

// Perform subscribe processing for all clients.
// Waits for the message to be received for the specified number of counts (if the message cannot be obtained, it is not counted).
// In this process, Subscribe is processed while continuing to publish.
func SubscribeAllClient(clients []*MQTT.Client, opts ExecOptions, param ...string) int {
	wg := new(sync.WaitGroup)

	results := make([]*SubscribeResult, len(clients))
	for id := 0; id < len(clients); id++ {
		wg.Add(1)

		client := clients[id]
		topic := fmt.Sprintf(opts.Topic+"/%d", id)

		results[id] = Subscribe(client, topic, opts.Qos)

		// DefaultHandler - Default usage the Subscribe individual Handler
		// DefaultHandler - Refer to the processing result of.
		if opts.UseDefaultHandler == true {
			results[id] = DefaultHandlerResults[id]
		}

		go func(clientId int) {
			defer wg.Done()

			var loop int = 0
			for results[clientId].Count < opts.Count {
				loop++

				if Debug {
					fmt.Printf("Subscribe : id=%d, count=%d, topic=%s\n", clientId, results[clientId].Count, topic)
				}

				if opts.IntervalTime > 0 {
					time.Sleep(time.Duration(opts.IntervalTime) * time.Millisecond)
				} else {
					// for to wait at least 1000 nanoseconds (0.001 milliseconds) to reduce the load on 
					time.Sleep(1000 * time.Nanosecond)
				}

				// To avoid an infinite loop, exit with an error when it reaches 100 times the specified Count.
				if loop >= opts.Count*100 {
					panic("Subscribe error : Not finished in the max count. It may not be received the message.")
				}
			}
		}(id)
	}

	wg.Wait()

	// Count the number of received messages
	totalCount := 0
	for id := 0; id < len(results); id++ {
		totalCount += results[id].Count
	}

	return totalCount
}

// Subscribe processing result
type SubscribeResult struct {
	Count int // Number of messages received
}

// Receive a message.
func Subscribe(client *MQTT.Client, topic string, qos byte) *SubscribeResult {
	var result *SubscribeResult = &SubscribeResult{}
	result.Count = 0

	var handler MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
		result.Count++
		if Debug {
			fmt.Printf("Received message : topic=%s, message=%s\n", msg.Topic(), msg.Payload())
		}
	}

	token := client.Subscribe(topic, qos, handler)

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Subscribe error: %s\n", token.Error())
	}

	return result
}

// Generate a fixed size message.
func CreateFixedSizeMessage(size int) string {
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(strconv.Itoa(i % 10))
	}

	message := buffer.String()
	return message
}

// Connects to the specified Broker and returns its MQTT client.
// Returns nil if the connection fails.
func Connect(id int, execOpts ExecOptions) *MQTT.Client {

	// If the Client ID is duplicated in multiple processes, 
	// it will be a problem on the Broker side,
	// so Allocate an ID using the process ID.
	// mqttbench <Hexadecimal value of process ID>-<Client serial number>
	pid := strconv.FormatInt(int64(os.Getpid()), 16)
	clientId := fmt.Sprintf("mqttbench%s-%d", pid, id)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(execOpts.Broker)
	opts.SetClientID(clientId)

	if execOpts.Username != "" {
		opts.SetUsername(execOpts.Username)
	}
	if execOpts.Password != "" {
		opts.SetPassword(execOpts.Password)
	}

	// TLS settings
	certConfig := execOpts.CertConfig
	switch c := certConfig.(type) {
	case ServerCertConfig:
		tlsConfig := CreateServerTlsConfig(c.ServerCertFile)
		opts.SetTLSConfig(tlsConfig)
	case ClientCertConfig:
		tlsConfig := CreateClientTlsConfig(c.RootCAFile, c.ClientCertFile, c.ClientKeyFile)
		opts.SetTLSConfig(tlsConfig)
	default:
		// do nothing.
	}

	if execOpts.UseDefaultHandler == true {
		// In the case of Apollo (using 1.7.1), you cannot subscribe unless DefaultPublishHandler is specified.
		// However, note that even if specified, 
		//the retained message will be retrieved only the first time and will be empty for the second and subsequent accesses.
		var result *SubscribeResult = &SubscribeResult{}
		result.Count = 0

		var handler MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
			result.Count++
			if Debug {
				fmt.Printf("Received at defaultHandler : topic=%s, message=%s\n", msg.Topic(), msg.Payload())
			}
		}
		opts.SetDefaultPublishHandler(handler)

		DefaultHandlerResults[id] = result
	}

	client := MQTT.NewClient(opts)
	token := client.Connect()

	if token.Wait() && token.Error() != nil {
		fmt.Printf("Connected error: %s\n", token.Error())
		return nil
	}

	return client
}

// Asynchronously disconnect from the Broker.
func AsyncDisconnect(clients []*MQTT.Client) {
	wg := new(sync.WaitGroup)

	for _, client := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Disconnect(client)
		}()
	}

	wg.Wait()
}

// Disconnect from the Broker.
func Disconnect(client *MQTT.Client) {
	client.Disconnect(10)
}

// Check the existence of the file.
// Returns true if the file exists, false if it does not exist.
// FilePath: The path of the file to check for existence
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func main() {
	broker := flag.String("broker", "tcp://{host}:{port}", "URI of MQTT broker (required)")
	action := flag.String("action", "p|pub or s|sub", "Publish or Subscribe or Subscribe(with publishing) (required)")
	qos := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retain := flag.Bool("retain", false, "MQTT Retain")
	topic := flag.String("topic", BASE_TOPIC, "Base topic")
	username := flag.String("broker-username", "", "Username for connecting to the MQTT broker")
	password := flag.String("broker-password", "", "Password for connecting to the MQTT broker")
	tls := flag.String("tls", "", "TLS mode. 'server:certFile' or 'client:rootCAFile,clientCertFile,clientKeyFile'")
	clients := flag.Int("clients", 10, "Number of clients")
	count := flag.Int("count", 100, "Number of loops per client")
	size := flag.Int("size", 1024, "Message size per publish (byte)")
	useDefaultHandler := flag.Bool("support-unknown-received", false, "Using default messageHandler for a message that does not match any known subscriptions")
	preTime := flag.Int("pretime", 3000, "Pre wait time (ms)")
	intervalTime := flag.Int("intervaltime", 0, "Interval time per message (ms)")
	debug := flag.Bool("x", false, "Debug mode")

	flag.Parse()

	if len(os.Args) <= 1 {
		flag.Usage()
		return
	}

	// validate "broker"
	if broker == nil || *broker == "" || *broker == "tcp://{host}:{port}" {
		fmt.Printf("Invalid argument : -broker -> %s\n", *broker)
		return
	}

	// validate "action"
	var method string = ""
	if *action == "p" || *action == "pub" {
		method = "pub"
	} else if *action == "s" || *action == "sub" {
		method = "sub"
	}

	if method != "pub" && method != "sub" {
		fmt.Printf("Invalid argument : -action -> %s\n", *action)
		return
	}

	// parse TLS mode
	var certConfig CertConfig = nil
	if *tls == "" {
		// nil
	} else if strings.HasPrefix(*tls, "server:") {
		var strArray = strings.Split(*tls, "server:")
		serverCertFile := strings.TrimSpace(strArray[1])
		if FileExists(serverCertFile) == false {
			fmt.Printf("File is not found. : certFile -> %s\n", serverCertFile)
			return
		}

		certConfig = ServerCertConfig{
			ServerCertFile: serverCertFile}
	} else if strings.HasPrefix(*tls, "client:") {
		var strArray = strings.Split(*tls, "client:")
		var configArray = strings.Split(strArray[1], ",")
		rootCAFile := strings.TrimSpace(configArray[0])
		clientCertFile := strings.TrimSpace(configArray[1])
		clientKeyFile := strings.TrimSpace(configArray[2])
		if FileExists(rootCAFile) == false {
			fmt.Printf("File is not found. : rootCAFile -> %s\n", rootCAFile)
			return
		}
		if FileExists(clientCertFile) == false {
			fmt.Printf("File is not found. : clientCertFile -> %s\n", clientCertFile)
			return
		}
		if FileExists(clientKeyFile) == false {
			fmt.Printf("File is not found. : clientKeyFile -> %s\n", clientKeyFile)
			return
		}

		certConfig = ClientCertConfig{
			RootCAFile:     rootCAFile,
			ClientCertFile: clientCertFile,
			ClientKeyFile:  clientKeyFile}
	} else {
		// nil
	}

	execOpts := ExecOptions{}
	execOpts.Broker = *broker
	execOpts.Qos = byte(*qos)
	execOpts.Retain = *retain
	execOpts.Topic = *topic
	execOpts.Username = *username
	execOpts.Password = *password
	execOpts.CertConfig = certConfig
	execOpts.ClientNum = *clients
	execOpts.Count = *count
	execOpts.MessageSize = *size
	execOpts.UseDefaultHandler = *useDefaultHandler
	execOpts.PreTime = *preTime
	execOpts.IntervalTime = *intervalTime

	Debug = *debug

	switch method {
	case "pub":
		Execute(PublishAllClient, execOpts)
	case "sub":
		Execute(SubscribeAllClient, execOpts)
	}
}
