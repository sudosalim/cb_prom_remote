package protocol

import (
	"fmt"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	pb "github.com/couchbase/cb_prom_remote/proto"
)

// CompressionType represents the type of compression used
type CompressionType string

const (
	CompressionSnappy CompressionType = "snappy"
	CompressionZstd   CompressionType = "zstd"
)

// Decoder handles decompression and deserialization of remote write requests
type Decoder struct {
	snappyDecoder *snappy.Reader
	zstdDecoder   *zstd.Decoder
}

// NewDecoder creates a new protocol decoder
func NewDecoder() (*Decoder, error) {
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &Decoder{
		zstdDecoder: zstdDecoder,
	}, nil
}

// DecodeWriteRequest decodes a compressed remote write request
func (d *Decoder) DecodeWriteRequest(data []byte, compressionType CompressionType) (*pb.WriteRequest, error) {
	var decompressed []byte
	var err error

	switch compressionType {
	case CompressionSnappy:
		decompressed, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress snappy data: %w", err)
		}
	case CompressionZstd:
		decompressed, err = d.zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress zstd data: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}

	var writeRequest pb.WriteRequest
	if err := proto.Unmarshal(decompressed, &writeRequest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return &writeRequest, nil
}

// DetectCompressionType detects compression type from Content-Encoding header
func DetectCompressionType(contentEncoding string) (CompressionType, error) {
	switch contentEncoding {
	case "snappy":
		return CompressionSnappy, nil
	case "zstd":
		return CompressionZstd, nil
	default:
		return "", fmt.Errorf("unsupported content encoding: %s", contentEncoding)
	}
}

// Close closes the decoder and releases resources
func (d *Decoder) Close() error {
	if d.zstdDecoder != nil {
		d.zstdDecoder.Close()
	}
	return nil
} 