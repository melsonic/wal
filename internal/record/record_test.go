package record

import (
	"bytes"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	seq := uint64(42)
	data := []byte("hello world")
	
	encoded := Encode(seq, data)
	if len(encoded) != HeaderSize+len(data) {
		t.Fatalf("expected length %d, got %d", HeaderSize+len(data), len(encoded))
	}
	
	rec, err := Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}
	
	if rec.Seq != seq {
		t.Errorf("expected seq %d, got %d", seq, rec.Seq)
	}
	
	if !bytes.Equal(rec.Data, data) {
		t.Errorf("expected data %q, got %q", data, rec.Data)
	}
	
	if rec.Length != uint32(len(data)) {
		t.Errorf("expected length %d, got %d", len(data), rec.Length)
	}
}

func TestDecodeCorrupted(t *testing.T) {
	seq := uint64(10)
	data := []byte("important data")
	
	encoded := Encode(seq, data)
	
	// corrupt the data
	encoded[HeaderSize+2] = 'x'
	
	_, err := Decode(encoded)
	if err == nil {
		t.Fatal("expected error on corrupted data, got none")
	}
}

func TestDecodeTooShort(t *testing.T) {
	shortBuf := make([]byte, HeaderSize-1)
	_, err := Decode(shortBuf)
	if err == nil {
		t.Fatal("expected error on short buffer, got none")
	}
}
