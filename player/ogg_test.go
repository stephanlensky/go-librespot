//go:build test_unit

package player

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// oggPage builds a minimal single-segment Ogg page. The checksum is left zero,
// which is fine because findLastOggPageEnd only inspects structure, not
// integrity.
func oggPage(serial, seq uint32, payload []byte) []byte {
	var b bytes.Buffer
	b.WriteString("OggS")
	b.WriteByte(0)                          // version
	b.WriteByte(0)                          // header type
	b.Write(make([]byte, 8))                // granule position
	_ = binary.Write(&b, binary.LittleEndian, serial)
	_ = binary.Write(&b, binary.LittleEndian, seq)
	b.Write(make([]byte, 4))                // checksum
	b.WriteByte(1)                          // one segment
	b.WriteByte(byte(len(payload)))         // segment size
	b.Write(payload)
	return b.Bytes()
}

func TestFindLastOggPageEnd(t *testing.T) {
	page1 := oggPage(1, 0, bytes.Repeat([]byte{0x01}, 100))
	page2 := oggPage(1, 1, bytes.Repeat([]byte{0x02}, 200))

	body := append(append([]byte{}, page1...), page2...)
	stream := append(body, 0x00, 0x00) // trailing CDN padding

	got := findLastOggPageEnd(bytes.NewReader(stream), int64(len(stream)))
	want := int64(len(body))
	if got != want {
		t.Fatalf("findLastOggPageEnd = %d, want %d", got, want)
	}
}

func TestFindLastOggPageEndNoTrailing(t *testing.T) {
	page1 := oggPage(1, 0, bytes.Repeat([]byte{0x01}, 100))
	page2 := oggPage(1, 1, bytes.Repeat([]byte{0x02}, 200))

	stream := append(append([]byte{}, page1...), page2...)

	got := findLastOggPageEnd(bytes.NewReader(stream), int64(len(stream)))
	if got != int64(len(stream)) {
		t.Fatalf("findLastOggPageEnd = %d, want %d", got, int64(len(stream)))
	}
}
