package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	total := 0
	for total < len(bytes){
		n, err := socket.Write(bytes[total:])
		if err != nil{
			return err
		}
		total += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	total := 0
	for total < size{
		n, err := socket.Read(buff[total:])
		total += n
		if err != nil {
			if total == size{
				break
			}
			return nil, err
		}
	}
	return buff[:total], nil
}
