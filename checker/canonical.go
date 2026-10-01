package main

import (
	"bytes"
	"fmt"

	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// checkCanonical requires b to be exactly one value in canonical atproto
// DAG-CBOR: go-ipld-prime's strict decoder (minimal integer/length heads,
// tag 42 only, no trailing bytes) must accept it, it must hold no floats
// (not in the atproto data model), and re-encoding it (definite lengths,
// map keys length-first then bytewise) must give back the same bytes. The
// last check catches unsorted or duplicate map keys and indefinite lengths.
func checkCanonical(b []byte) error {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := (dagcbor.DecodeOptions{AllowLinks: true}).Decode(nb, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("not DAG-CBOR: %w", err)
	}
	n := nb.Build()
	if err := noFloats(n); err != nil {
		return err
	}
	var out bytes.Buffer
	enc := dagcbor.EncodeOptions{AllowLinks: true, MapSortMode: codec.MapSortMode_RFC7049}
	if err := enc.Encode(n, &out); err != nil {
		return fmt.Errorf("re-encoding: %w", err)
	}
	if !bytes.Equal(out.Bytes(), b) {
		at := 0
		for at < len(b) && at < out.Len() && b[at] == out.Bytes()[at] {
			at++
		}
		return fmt.Errorf("not canonical DAG-CBOR: re-encoding differs at byte %d of %d", at, len(b))
	}
	return nil
}

func noFloats(n datamodel.Node) error {
	switch n.Kind() {
	case datamodel.Kind_Float:
		return fmt.Errorf("float in DAG-CBOR (not in the atproto data model)")
	case datamodel.Kind_Map:
		it := n.MapIterator()
		for !it.Done() {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := noFloats(v); err != nil {
				return err
			}
		}
	case datamodel.Kind_List:
		it := n.ListIterator()
		for !it.Done() {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := noFloats(v); err != nil {
				return err
			}
		}
	}
	return nil
}
