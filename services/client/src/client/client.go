package client

import (
	"bufio"
	"errors"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"
	"runtime"
	"context"
	"syscall"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 20
const CONNECTION_ATTEMPS_DELAY_MS = 500

type ClientConfig struct {
	ServerHost 	string
	ServerPort 	string
	AgencyId   	string
	InputFile  	string
	OutputFile 	string
	BatchSize 	int
}

type Client struct {
	conn    net.Conn
	config  ClientConfig
	ctx 	context.Context
	cancel	context.CancelFunc
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{conn: conn, config: config, ctx: ctx, cancel: cancel}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	//Al recibir SIGTERM se cancela el contexto y se cierra la conexion para 
	//desbloquear operaciones de red que podrian estar esperando indefinidamente
	go func(){
		select{
		case <- sigChan:
			logger.Info("sigterm", logger.InProgress, "agency-id", config.AgencyId)
			cancel()
			client.conn.Close()
		case <- ctx.Done():

		}
	}()

	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func sendBatchAndWaitAck(batch [][]byte, client *Client) error {
	if len(batch) == 0 {
		return nil
	}

	if err := protocol.SendBatch(client.conn, batch); err != nil {
		logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	tag, ackData, err := protocol.Recv(client.conn)
	if err != nil {
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	if tag != protocol.BatchAck || len(ackData) == 0 || ackData[0] != 0 {
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "tag", tag)
		return errors.New("server rejected batch")
	}

	return nil
}

func (client *Client) Run() error {
	const mainAction = "process-input-file"
	defer client.conn.Close()
	defer client.cancel()

	logger.Info(
		mainAction, logger.InProgress,
		"agency-id", client.config.AgencyId,
		"input-file", client.config.InputFile,
		"output-file", client.config.OutputFile,
	)

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	defer inputFile.Close()

	if err := os.MkdirAll(filepath.Dir(client.config.OutputFile), 0755); err != nil {
		logger.Error("create-output-dir", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	defer outputFile.Close()

	agency, _ := strconv.Atoi(client.config.AgencyId)
	if err := protocol.SendAgency(client.conn, agency); err != nil {
		if client.ctx.Err() != nil{
			return nil
		}
		logger.Error("send-agency", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	totalBetsSent := 0
	batch := make([][]byte, 0, client.config.BatchSize)
	flushCount := 0

	//Se acumulan lineas completas hasta BatchSize para reducir la cantidad de mensajes TCP
	for scanner.Scan() {
		select{
		case <-client.ctx.Done():
			return nil
		default:

		}
		line := append([]byte(nil), scanner.Bytes()...)
		batch = append(batch, line)
		totalBetsSent++

		if len(batch) == client.config.BatchSize {
			// var flushErr error
			if err := sendBatchAndWaitAck(batch, client); err != nil{
				return err
			}
			
			batch = batch[:0]

			flushCount++
			if flushCount % 64 == 0 {
				runtime.GC()
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := sendBatchAndWaitAck(batch, client); err != nil{
		if client.ctx.Err() != nil{
			return nil
		}
		return err
	}

	if err := protocol.SendFinish(client.conn); err != nil {
		if client.ctx.Err() != nil{
				return nil
			}
		
		logger.Error("send-finish", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	for {
		tag, data, err := protocol.Recv(client.conn)
		if err != nil {
			if client.ctx.Err() != nil{
				return nil
			}
			logger.Error("recv-winner", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
			return err
		}
		if tag == protocol.Finish {
			break
		}
		if tag == protocol.Winner {
			if _, err := outputFile.Write(append(data, '\n')); err != nil {
				logger.Error("write-output-line", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
				return err
			}
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId, "messages-amount", totalBetsSent)
	return nil
}

