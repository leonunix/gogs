package transfer

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	pktFlush      = 0
	pktDelim      = 1
	maxPktDataLen = 65516 // 65520 - 4 byte header
)

// PktlineScanner reads pkt-line formatted packets.
type PktlineScanner struct {
	r *bufio.Reader
}

// NewPktlineScanner creates a new pkt-line scanner from the given reader.
func NewPktlineScanner(r io.Reader) *PktlineScanner {
	return &PktlineScanner{r: bufio.NewReaderSize(r, 65520)}
}

// ReadPacket reads a single pkt-line packet and returns the raw data and
// packet type. For flush packets it returns (nil, pktFlush, nil), for
// delimiter packets (nil, pktDelim, nil).
func (s *PktlineScanner) ReadPacket() ([]byte, int, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(s.r, lenBuf[:])
	if err != nil {
		return nil, 0, err
	}

	pktLen, err := strconv.ParseUint(string(lenBuf[:]), 16, 16)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid pkt-line length: %q", string(lenBuf[:]))
	}

	if pktLen == 0 {
		return nil, pktFlush, nil
	}
	if pktLen == 1 {
		return nil, pktDelim, nil
	}
	if pktLen < 4 {
		return nil, 0, fmt.Errorf("invalid pkt-line length: %d", pktLen)
	}

	data := make([]byte, pktLen-4)
	_, err = io.ReadFull(s.r, data)
	if err != nil {
		return nil, 0, err
	}
	return data, int(pktLen), nil
}

// ReadPacketText reads a text pkt-line and strips the trailing newline.
// It returns the text and packet type (pktFlush, pktDelim, or >1 for data).
func (s *PktlineScanner) ReadPacketText() (string, int, error) {
	data, pktType, err := s.ReadPacket()
	if err != nil || pktType <= 1 {
		return "", pktType, err
	}

	text := string(data)
	if len(text) > 0 && text[len(text)-1] == '\n' {
		text = text[:len(text)-1]
	}
	return text, pktType, nil
}

// ReadPacketListToFlush reads text packets until a flush packet.
func (s *PktlineScanner) ReadPacketListToFlush() ([]string, error) {
	var list []string
	for {
		text, pktType, err := s.ReadPacketText()
		if err != nil {
			return nil, err
		}
		if pktType == pktFlush {
			return list, nil
		}
		list = append(list, text)
	}
}

// ReadPacketListToDelim reads text packets until a delimiter packet.
func (s *PktlineScanner) ReadPacketListToDelim() ([]string, error) {
	var list []string
	for {
		text, pktType, err := s.ReadPacketText()
		if err != nil {
			return nil, err
		}
		if pktType == pktDelim {
			return list, nil
		}
		if pktType == pktFlush {
			return nil, fmt.Errorf("unexpected flush packet while reading to delimiter")
		}
		list = append(list, text)
	}
}

// DataReader returns an io.Reader that reads binary data from consecutive
// pkt-line packets until a flush packet terminates the stream.
func (s *PktlineScanner) DataReader() io.Reader {
	return &pktlineDataReader{scanner: s}
}

type pktlineDataReader struct {
	scanner *PktlineScanner
	buf     []byte
	done    bool
}

func (r *pktlineDataReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}

	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	data, pktType, err := r.scanner.ReadPacket()
	if err != nil {
		return 0, err
	}
	if pktType == pktFlush {
		r.done = true
		return 0, io.EOF
	}

	n := copy(p, data)
	if n < len(data) {
		r.buf = data[n:]
	}
	return n, nil
}

// PktlineWriter writes pkt-line formatted packets.
type PktlineWriter struct {
	w io.Writer
}

// NewPktlineWriter creates a new pkt-line writer.
func NewPktlineWriter(w io.Writer) *PktlineWriter {
	return &PktlineWriter{w: w}
}

// WritePacketText writes a text line as a pkt-line with a trailing newline.
func (w *PktlineWriter) WritePacketText(text string) error {
	return w.WritePacket([]byte(text + "\n"))
}

// WritePacket writes raw bytes as a single pkt-line.
func (w *PktlineWriter) WritePacket(data []byte) error {
	if len(data) > maxPktDataLen {
		return fmt.Errorf("packet data too large: %d > %d", len(data), maxPktDataLen)
	}
	header := fmt.Sprintf("%04x", len(data)+4)
	if _, err := io.WriteString(w.w, header); err != nil {
		return err
	}
	_, err := w.w.Write(data)
	return err
}

// WriteFlush writes a flush packet (0000).
func (w *PktlineWriter) WriteFlush() error {
	_, err := io.WriteString(w.w, "0000")
	return err
}

// WriteDelim writes a delimiter packet (0001).
func (w *PktlineWriter) WriteDelim() error {
	_, err := io.WriteString(w.w, "0001")
	return err
}

// DataWriter returns a writer that splits data into pkt-line packets.
// Call Flush() when done to send the terminating flush packet.
func (w *PktlineWriter) DataWriter() *PktlineDataWriter {
	return &PktlineDataWriter{pw: w}
}

// PktlineDataWriter writes binary data as a sequence of pkt-line packets.
type PktlineDataWriter struct {
	pw *PktlineWriter
}

// Write splits p into pkt-line-sized chunks and writes them.
func (dw *PktlineDataWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxPktDataLen {
			chunk = p[:maxPktDataLen]
		}
		if err := dw.pw.WritePacket(chunk); err != nil {
			return total, err
		}
		total += len(chunk)
		p = p[len(chunk):]
	}
	return total, nil
}

// Flush writes the terminating flush packet.
func (dw *PktlineDataWriter) Flush() error {
	return dw.pw.WriteFlush()
}
