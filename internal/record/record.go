// Package record provides the low-level binary encoding and decoding for WAL entries.
//
// Because the WAL is an append-only binary file, we need a reliable way to frame
// our data (know where one record ends and the next begins) and ensure its integrity
// (detect partial writes or bit flips).
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Record represents a single entry in the Write-Ahead Log.
//
// When written to disk, it is serialized in a specific binary format:
//
//	+---------+------------+------------------+
//	| CRC (4) | Length (4) |    Data (N)      |
//	+---------+------------+------------------+
//
// 1. CRC (4 bytes, Little Endian): A checksum calculated over Length + Data.
// 2. Length (4 bytes, Little Endian): The size of the Data payload in bytes.
// 3. Data (N bytes): The actual user payload.
type Record struct {
	CRC    uint32 // CRC32 checksum for data integrity
	Length uint32 // Length of the data payload
	Data   []byte // The actual data appended by the user
}

const (
	CRCSize          = 4
	RecordSizeLength = 4
	// HeaderSize is the size of the record header (CRC + Length).
	HeaderSize = CRCSize + RecordSizeLength
)

// Encode encodes the data into a binary record format suitable for writing to disk.
func Encode(data []byte) []byte {
	recordBuffer := make([]byte, HeaderSize+len(data))
	binary.LittleEndian.PutUint32(recordBuffer[CRCSize:RecordSizeLength], uint32(len(data)))
	copy(recordBuffer[HeaderSize:], data)
	checkSum := crc32.ChecksumIEEE(recordBuffer[CRCSize:])
	binary.LittleEndian.PutUint32(recordBuffer[0:CRCSize], checkSum)
	return recordBuffer
}

// Decode decodes a binary record back into its components and verifies the CRC.
func Decode(buf []byte) (*Record, error) {
	if len(buf) < HeaderSize {
		return nil, errors.New("size less than header size")
	}
	originalCheckSum := binary.LittleEndian.Uint32(buf[0:CRCSize])
	dataLength := binary.LittleEndian.Uint32(buf[CRCSize:RecordSizeLength])
	data := buf[HeaderSize:]
	calculatedCheckSum := crc32.ChecksumIEEE(buf[CRCSize:])
	if originalCheckSum != calculatedCheckSum {
		return nil, errors.New("corrupted data!!! checksum doesn't match")
	}
	record := Record{
		CRC:    originalCheckSum,
		Length: dataLength,
		Data:   data,
	}
	return &record, nil
}
