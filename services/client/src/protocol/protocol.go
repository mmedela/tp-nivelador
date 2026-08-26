package protocol

import (
	"encoding/binary"
	"io"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	Agency   byte = 1
	Batch    byte = 2
	Finish   byte = 3
	Winner   byte = 4
	BatchAck byte = 5 
)

func send(w io.Writer, tag byte, data []byte) error {
	payload := append([]byte{tag}, data...)
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	return safe_socket.SendAll(w, frame)
}

func Recv(r io.Reader) (byte, []byte, error) {
	lb, err := safe_socket.RecvAll(r, 4)
	if err != nil { return 0, nil, err }
	payload, err := safe_socket.RecvAll(r, int(binary.BigEndian.Uint32(lb)))
	if err != nil { return 0, nil, err }
	return payload[0], payload[1:], nil
}

func SendAgency(w io.Writer, agency int) error {
	d := make([]byte, 4); binary.BigEndian.PutUint32(d, uint32(agency)); return send(w, Agency, d)
}
func SendBatch(w io.Writer, csv []byte) error { return send(w, Batch, csv) }
func SendFinish(w io.Writer) error          { return send(w, Finish, nil) }