package transfer

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	log "unknwon.dev/clog/v2"

	"gogs.io/gogs/internal/database"
	"gogs.io/gogs/internal/lfsx"
)

// Serve runs the Git LFS pure SSH transfer protocol on the given
// reader/writer pair. The operation must be "upload" or "download".
func Serve(
	ctx context.Context,
	r io.Reader,
	w io.Writer,
	op string,
	repo *database.Repository,
	defaultStorage lfsx.Storage,
	storagers map[lfsx.Storage]lfsx.Storager,
) error {
	scanner := NewPktlineScanner(r)
	writer := NewPktlineWriter(w)

	// Capability advertisement.
	if err := writer.WritePacketText("version=1"); err != nil {
		return errors.Wrap(err, "send capability")
	}
	if err := writer.WriteFlush(); err != nil {
		return errors.Wrap(err, "flush capabilities")
	}

	p := &processor{
		ctx:            ctx,
		scanner:        scanner,
		writer:         writer,
		op:             op,
		repo:           repo,
		defaultStorage: defaultStorage,
		storagers:      storagers,
	}
	return p.processCommands()
}

type processor struct {
	ctx            context.Context
	scanner        *PktlineScanner
	writer         *PktlineWriter
	op             string
	repo           *database.Repository
	defaultStorage lfsx.Storage
	storagers      map[lfsx.Storage]lfsx.Storager
}

func (p *processor) processCommands() error {
	for {
		text, pktType, err := p.scanner.ReadPacketText()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if pktType <= 1 {
			continue
		}

		parts := strings.SplitN(text, " ", 2)
		cmd := parts[0]
		var arg string
		if len(parts) > 1 {
			arg = parts[1]
		}

		log.Trace("[LFS SSH] Received command %q %q", cmd, arg)

		switch cmd {
		case "version":
			err = p.handleVersion(arg)
		case "batch":
			err = p.handleBatch()
		case "put-object":
			err = p.handlePutObject(arg)
		case "verify-object":
			err = p.handleVerifyObject(arg)
		case "get-object":
			err = p.handleGetObject(arg)
		case "quit":
			_, _ = p.scanner.ReadPacketListToFlush()
			_ = p.sendStatusOK()
			return nil
		default:
			err = p.sendError(400, fmt.Sprintf("unknown command: %s", cmd))
		}

		if err != nil {
			log.Error("[LFS SSH] Error handling %q: %v", cmd, err)
			_ = p.sendError(500, "internal server error")
			return err
		}
	}
}

func (p *processor) handleVersion(version string) error {
	_, err := p.scanner.ReadPacketListToFlush()
	if err != nil {
		return err
	}
	if version != "1" {
		return p.sendError(400, "unsupported version")
	}
	return p.sendStatusOK()
}

func (p *processor) handleBatch() error {
	argLines, err := p.scanner.ReadPacketListToDelim()
	if err != nil {
		return err
	}
	args := parseArgs(argLines)

	if algo := args["hash-algo"]; algo != "" && algo != "sha256" {
		return p.sendError(400, fmt.Sprintf("unsupported hash algorithm: %s", algo))
	}

	oidLines, err := p.scanner.ReadPacketListToFlush()
	if err != nil {
		return err
	}

	type batchItem struct {
		oid  string
		size int64
	}
	items := make([]batchItem, 0, len(oidLines))
	oids := make([]lfsx.OID, 0, len(oidLines))
	for _, line := range oidLines {
		fields := strings.SplitN(line, " ", 2)
		if len(fields) < 2 {
			return p.sendError(400, "invalid batch line")
		}
		size, parseErr := strconv.ParseInt(fields[1], 10, 64)
		if parseErr != nil {
			return p.sendError(400, "invalid size in batch line")
		}
		items = append(items, batchItem{oid: fields[0], size: size})
		oids = append(oids, lfsx.OID(fields[0]))
	}

	existing, err := database.Handle.LFS().GetObjectsByOIDs(p.ctx, p.repo.ID, oids...)
	if err != nil {
		log.Error("[LFS SSH] GetObjectsByOIDs: %v", err)
		return p.sendError(500, "internal server error")
	}
	existingSet := make(map[lfsx.OID]*database.LFSObject, len(existing))
	for _, obj := range existing {
		existingSet[obj.OID] = obj
	}

	if err := p.writer.WritePacketText("status 200"); err != nil {
		return err
	}
	if err := p.writer.WriteDelim(); err != nil {
		return err
	}

	for _, item := range items {
		obj := existingSet[lfsx.OID(item.oid)]
		var action string
		size := item.size
		switch p.op {
		case "upload":
			if obj != nil {
				action = "noop"
				size = obj.Size
			} else {
				action = "upload"
			}
		case "download":
			if obj != nil {
				action = "download"
				size = obj.Size
			} else {
				action = "noop"
			}
		}
		if err := p.writer.WritePacketText(fmt.Sprintf("%s %d %s", item.oid, size, action)); err != nil {
			return err
		}
	}
	return p.writer.WriteFlush()
}

func (p *processor) handlePutObject(oid string) error {
	if !lfsx.ValidOID(lfsx.OID(oid)) {
		drainPutObject(p.scanner)
		return p.sendError(400, "invalid oid")
	}

	argLines, err := p.scanner.ReadPacketListToDelim()
	if err != nil {
		return err
	}
	args := parseArgs(argLines)

	sizeStr, ok := args["size"]
	if !ok {
		_, _ = io.Copy(io.Discard, p.scanner.DataReader())
		return p.sendError(400, "missing size argument")
	}
	expectedSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		_, _ = io.Copy(io.Discard, p.scanner.DataReader())
		return p.sendError(400, "invalid size")
	}

	s := p.storagers[p.defaultStorage]
	written, err := s.Upload(lfsx.OID(oid), io.NopCloser(p.scanner.DataReader()))
	if err != nil {
		if errors.Is(err, lfsx.ErrOIDMismatch) || errors.Is(err, lfsx.ErrInvalidOID) {
			return p.sendError(400, err.Error())
		}
		log.Error("[LFS SSH] Upload %q: %v", oid, err)
		return p.sendError(500, "upload failed")
	}

	if written != expectedSize {
		return p.sendError(400, "size mismatch")
	}

	err = database.Handle.LFS().CreateObject(p.ctx, p.repo.ID, lfsx.OID(oid), written, s.Storage())
	if err != nil {
		log.Trace("[LFS SSH] CreateObject (may be duplicate): %v", err)
	}

	log.Trace("[LFS SSH] Object uploaded %q (%d bytes)", oid, written)
	return p.sendStatusOK()
}

func (p *processor) handleVerifyObject(oid string) error {
	argLines, err := p.scanner.ReadPacketListToFlush()
	if err != nil {
		return err
	}

	if !lfsx.ValidOID(lfsx.OID(oid)) {
		return p.sendError(400, "invalid oid")
	}

	args := parseArgs(argLines)
	sizeStr, ok := args["size"]
	if !ok {
		return p.sendError(400, "missing size argument")
	}
	expectedSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return p.sendError(400, "invalid size")
	}

	obj, err := database.Handle.LFS().GetObjectByOID(p.ctx, p.repo.ID, lfsx.OID(oid))
	if err != nil {
		if database.IsErrLFSObjectNotExist(err) {
			return p.sendError(404, "object not found")
		}
		log.Error("[LFS SSH] GetObjectByOID %q: %v", oid, err)
		return p.sendError(500, "internal server error")
	}

	if obj.Size != expectedSize {
		return p.sendError(409, "size mismatch")
	}
	return p.sendStatusOK()
}

func (p *processor) handleGetObject(oid string) error {
	_, err := p.scanner.ReadPacketListToFlush()
	if err != nil {
		return err
	}

	if !lfsx.ValidOID(lfsx.OID(oid)) {
		return p.sendError(400, "invalid oid")
	}

	obj, err := database.Handle.LFS().GetObjectByOID(p.ctx, p.repo.ID, lfsx.OID(oid))
	if err != nil {
		if database.IsErrLFSObjectNotExist(err) {
			return p.sendError(404, fmt.Sprintf("object %s not found", oid))
		}
		log.Error("[LFS SSH] GetObjectByOID %q: %v", oid, err)
		return p.sendError(500, "internal server error")
	}

	s := p.storagers[obj.Storage]
	if s == nil {
		log.Error("[LFS SSH] Storage %q not found for object %q", obj.Storage, oid)
		return p.sendError(500, "storage backend not available")
	}

	if err := p.writer.WritePacketText("status 200"); err != nil {
		return err
	}
	if err := p.writer.WritePacketText(fmt.Sprintf("size=%d", obj.Size)); err != nil {
		return err
	}
	if err := p.writer.WriteDelim(); err != nil {
		return err
	}

	dw := p.writer.DataWriter()
	if err := s.Download(obj.OID, dw); err != nil {
		log.Error("[LFS SSH] Download %q: %v", oid, err)
		return err
	}
	return dw.Flush()
}

func (p *processor) sendStatusOK() error {
	if err := p.writer.WritePacketText("status 200"); err != nil {
		return err
	}
	return p.writer.WriteFlush()
}

func (p *processor) sendError(code int, message string) error {
	if err := p.writer.WritePacketText(fmt.Sprintf("status %03d", code)); err != nil {
		return err
	}
	if message != "" {
		if err := p.writer.WriteDelim(); err != nil {
			return err
		}
		if err := p.writer.WritePacketText(message); err != nil {
			return err
		}
	}
	return p.writer.WriteFlush()
}

// drainPutObject consumes the remainder of a put-object request (args +
// delimiter + binary data + flush) so the protocol stream stays in sync.
func drainPutObject(s *PktlineScanner) {
	_, _ = s.ReadPacketListToDelim()
	_, _ = io.Copy(io.Discard, s.DataReader())
}

func parseArgs(lines []string) map[string]string {
	args := make(map[string]string, len(lines))
	for _, line := range lines {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			args[k] = v
		}
	}
	return args
}
