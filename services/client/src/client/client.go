package client

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 20
const CONNECTION_ATTEMPS_DELAY_MS = 500

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
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

func (client *Client) Run() error {
	const mainAction = "process-input-file"
	defer client.conn.Close()

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
		logger.Error("send-agency", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	reader := bufio.NewReader(inputFile)
	messageId := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			logger.Error("read-input-line", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId, "err", err)
			return err
		}
		if len(line) > 0 {
			if err := protocol.SendBet(client.conn, []byte(strings.TrimRight(line, "\n"))); err != nil {
				logger.Error("send-bet", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId, "err", err)
				return err
			}
			messageId++
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if err := protocol.SendFinish(client.conn); err != nil {
		logger.Error("send-finish", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	for {
		tag, data, err := protocol.Recv(client.conn)
		if err != nil {
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

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId, "messages-amount", messageId)
	return nil
}

