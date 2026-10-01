package main

import (
	"bytes"
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// Frame op values from the event-stream spec.
const (
	frameOpMessage = 1
	frameOpError   = -1
)

type frameHeader struct {
	Op int64
	T  string
}

// event is one decoded firehose frame. Exactly one of the typed fields is set
// for message frames; ErrName/ErrMsg are set for error frames.
type event struct {
	Header   frameHeader
	Commit   *comatproto.SyncSubscribeRepos_Commit
	Sync     *comatproto.SyncSubscribeRepos_Sync
	Identity *comatproto.SyncSubscribeRepos_Identity
	Account  *comatproto.SyncSubscribeRepos_Account
	Info     *comatproto.SyncSubscribeRepos_Info
	ErrName  string
	ErrMsg   string
	// NonCanonical is set when the header or body is not canonical DAG-CBOR.
	NonCanonical string
}

// seq returns the event's sequence number, or (0, false) if it has none.
func (e *event) seq() (int64, bool) {
	switch {
	case e.Commit != nil:
		return e.Commit.Seq, true
	case e.Sync != nil:
		return e.Sync.Seq, true
	case e.Identity != nil:
		return e.Identity.Seq, true
	case e.Account != nil:
		return e.Account.Seq, true
	}
	return 0, false
}

func (e *event) did() string {
	switch {
	case e.Commit != nil:
		return e.Commit.Repo
	case e.Sync != nil:
		return e.Sync.Did
	case e.Identity != nil:
		return e.Identity.Did
	case e.Account != nil:
		return e.Account.Did
	}
	return ""
}

func (e *event) time() string {
	switch {
	case e.Commit != nil:
		return e.Commit.Time
	case e.Sync != nil:
		return e.Sync.Time
	case e.Identity != nil:
		return e.Identity.Time
	case e.Account != nil:
		return e.Account.Time
	}
	return ""
}

// decodeFrame parses one websocket binary message: a DAG-CBOR header object
// followed immediately by a DAG-CBOR body object.
func decodeFrame(msg []byte) (*event, error) {
	br := bytes.NewReader(msg)
	cr := cbg.NewCborReader(br) // reads br byte by byte (no buffering)
	hdr, err := readHeader(cr)
	if err != nil {
		return nil, fmt.Errorf("frame header: %w", err)
	}
	ev := &event{Header: hdr}
	hlen := len(msg) - br.Len()
	if err := checkCanonical(msg[:hlen]); err != nil {
		ev.NonCanonical = "frame header: " + err.Error()
	} else if err := checkCanonical(msg[hlen:]); err != nil {
		ev.NonCanonical = "frame body: " + err.Error()
	}
	switch hdr.Op {
	case frameOpError:
		m, err := readStringMap(cr)
		if err != nil {
			return nil, fmt.Errorf("error frame body: %w", err)
		}
		ev.ErrName, ev.ErrMsg = m["error"], m["message"]
		return ev, nil
	case frameOpMessage:
	default:
		return nil, fmt.Errorf("unknown frame op %d", hdr.Op)
	}

	switch hdr.T {
	case "#commit":
		ev.Commit = new(comatproto.SyncSubscribeRepos_Commit)
		err = ev.Commit.UnmarshalCBOR(cr)
	case "#sync":
		ev.Sync = new(comatproto.SyncSubscribeRepos_Sync)
		err = ev.Sync.UnmarshalCBOR(cr)
	case "#identity":
		ev.Identity = new(comatproto.SyncSubscribeRepos_Identity)
		err = ev.Identity.UnmarshalCBOR(cr)
	case "#account":
		ev.Account = new(comatproto.SyncSubscribeRepos_Account)
		err = ev.Account.UnmarshalCBOR(cr)
	case "#info":
		ev.Info = new(comatproto.SyncSubscribeRepos_Info)
		err = ev.Info.UnmarshalCBOR(cr)
	default:
		// Unknown message types must be ignored by consumers per spec.
		return ev, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s body: %w", hdr.T, err)
	}
	return ev, nil
}

func readHeader(cr *cbg.CborReader) (frameHeader, error) {
	var h frameHeader
	maj, n, err := cr.ReadHeader()
	if err != nil {
		return h, err
	}
	if maj != cbg.MajMap {
		return h, fmt.Errorf("header is not a map (major %d)", maj)
	}
	seenOp := false
	for i := uint64(0); i < n; i++ {
		key, err := cbg.ReadString(cr)
		if err != nil {
			return h, err
		}
		switch key {
		case "op":
			maj, v, err := cr.ReadHeader()
			if err != nil {
				return h, err
			}
			switch maj {
			case cbg.MajUnsignedInt:
				h.Op = int64(v)
			case cbg.MajNegativeInt:
				h.Op = -1 - int64(v)
			default:
				return h, fmt.Errorf("header op has major type %d", maj)
			}
			seenOp = true
		case "t":
			if h.T, err = cbg.ReadString(cr); err != nil {
				return h, err
			}
		default:
			var d cbg.Deferred
			if err := d.UnmarshalCBOR(cr); err != nil {
				return h, err
			}
		}
	}
	if !seenOp {
		return h, fmt.Errorf("header missing op")
	}
	return h, nil
}

// readStringMap reads a CBOR map, keeping string-valued entries and skipping others.
func readStringMap(cr *cbg.CborReader) (map[string]string, error) {
	maj, n, err := cr.ReadHeader()
	if err != nil {
		return nil, err
	}
	if maj != cbg.MajMap {
		return nil, fmt.Errorf("not a map (major %d)", maj)
	}
	out := map[string]string{}
	for i := uint64(0); i < n; i++ {
		key, err := cbg.ReadString(cr)
		if err != nil {
			return nil, err
		}
		var d cbg.Deferred
		if err := d.UnmarshalCBOR(cr); err != nil {
			return nil, err
		}
		if s, err := cbg.ReadString(cbg.NewCborReader(bytes.NewReader(d.Raw))); err == nil {
			out[key] = s
		}
	}
	return out, nil
}
