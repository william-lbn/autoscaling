//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const normalOrdinaryUdevV2MaximumProperties = 64
const normalOrdinaryUdevV2MaximumBody = 8152
const normalOrdinaryUdevV2HeaderBytes = 40

type normalOrdinaryUdevV2Request struct {
	Kind, Action uint16
	Index        uint32
	Sequence     uint64
	Properties   map[string]string
	Raw          []byte
}

// 正文只有selector和processed属性，绝不携来源/路径能力；Source另核原kernel票据。
func decodeNormalOrdinaryUdevV2Request(body []byte) (normalOrdinaryUdevV2Request, error) {
	var q normalOrdinaryUdevV2Request
	if len(body) < 40 || len(body) > 8192 || string(body[:8]) != "NDSGUE01" || binary.BigEndian.Uint32(body[8:12]) != 1 || binary.BigEndian.Uint32(body[36:40]) != 0 {
		return q, fmt.Errorf("udev fixed request framing differs")
	}
	q.Kind = binary.BigEndian.Uint16(body[12:14])
	q.Action = binary.BigEndian.Uint16(body[14:16])
	q.Index = binary.BigEndian.Uint32(body[16:20])
	q.Sequence = binary.BigEndian.Uint64(body[20:28])
	count := binary.BigEndian.Uint32(body[28:32])
	size := binary.BigEndian.Uint32(body[32:36])
	if q.Kind < 1 || q.Kind > 3 || q.Kind != 2 && q.Index != 0 || q.Action < 1 || q.Action > 8 || q.Sequence == 0 || size != uint32(len(body)-40) || count == 0 || count > 64 {
		return normalOrdinaryUdevV2Request{}, fmt.Errorf("udev fixed request selectors differ")
	}
	p, e := normalOrdinaryUdevV2Properties(body[40:], true)
	if e != nil || len(p) != int(count) {
		return normalOrdinaryUdevV2Request{}, fmt.Errorf("udev canonical properties differ: %w", e)
	}
	q.Properties = p
	q.Raw = bytes.Clone(body[40:])
	if p["ACTION"] != normalOrdinaryUdevV2Action(q.Action) || p["SEQNUM"] != strconv.FormatUint(q.Sequence, 10) {
		return normalOrdinaryUdevV2Request{}, fmt.Errorf("udev action/sequence selector differs from properties")
	}
	return q, nil
}
func normalOrdinaryUdevV2Action(action uint16) string {
	all := []string{"", "add", "change", "remove", "online", "offline", "bind", "unbind", "move"}
	if int(action) >= len(all) {
		return ""
	}
	return all[action]
}
func normalOrdinaryUdevV2Properties(raw []byte, sorted bool) (map[string]string, error) {
	if len(raw) == 0 || len(raw) > normalOrdinaryUdevV2MaximumBody || raw[len(raw)-1] != 0 || !utf8.Valid(raw) {
		return nil, fmt.Errorf("bounded NUL properties required")
	}
	fields := bytes.Split(raw[:len(raw)-1], []byte{0})
	if len(fields) > 64 {
		return nil, fmt.Errorf("udev property count exceeds bound")
	}
	out := map[string]string{}
	last := ""
	for _, field := range fields {
		key, value, ok := strings.Cut(string(field), "=")
		if !ok || len(key) == 0 || len(key) > 64 || len(value) > 1024 || sorted && last != "" && key <= last {
			return nil, fmt.Errorf("udev property order/length differs")
		}
		for _, c := range []byte(key) {
			if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				return nil, fmt.Errorf("udev property key is not canonical")
			}
		}
		for _, c := range value {
			if c < 32 || c == 127 {
				return nil, fmt.Errorf("udev property value contains controls")
			}
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("udev duplicate property")
		}
		out[key] = value
		last = key
	}
	return out, nil
}
func normalOrdinaryUdevV2CanonicalProperties(p map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, []byte(k+"="+p[k])...)
		b = append(b, 0)
	}
	if _, e := normalOrdinaryUdevV2Properties(b, true); e != nil {
		return nil, e
	}
	return b, nil
}

// 原libudev MurmurHash2(seed0)，标准40B公开header；只给Root已核票据调用。
func normalOrdinaryUdevV2Hash(s string) uint32 {
	data := []byte(s)
	h := uint32(len(data))
	for len(data) >= 4 {
		k := binary.LittleEndian.Uint32(data[:4])
		k *= 0x5bd1e995
		k ^= k >> 24
		k *= 0x5bd1e995
		h *= 0x5bd1e995
		h ^= k
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= 0x5bd1e995
	}
	h ^= h >> 13
	h *= 0x5bd1e995
	h ^= h >> 15
	return h
}
func normalOrdinaryUdevV2PublicPacket(properties map[string]string) ([]byte, error) {
	raw, e := normalOrdinaryUdevV2CanonicalProperties(properties)
	if e != nil {
		return nil, e
	}
	subsystem := properties["SUBSYSTEM"]
	if subsystem == "" {
		return nil, fmt.Errorf("udev public subsystem absent")
	}
	header := make([]byte, 40)
	copy(header, "libudev\x00")
	binary.BigEndian.PutUint32(header[8:12], 0xfeedcafe)
	binary.LittleEndian.PutUint32(header[12:16], 40)
	binary.LittleEndian.PutUint32(header[16:20], 40)
	binary.LittleEndian.PutUint32(header[20:24], uint32(len(raw)))
	binary.BigEndian.PutUint32(header[24:28], normalOrdinaryUdevV2Hash(subsystem))
	if properties["DEVTYPE"] != "" {
		binary.BigEndian.PutUint32(header[28:32], normalOrdinaryUdevV2Hash(properties["DEVTYPE"]))
	}
	var bloom uint64
	if tags := properties["TAGS"]; tags != "" {
		if !strings.HasPrefix(tags, ":") || !strings.HasSuffix(tags, ":") {
			return nil, fmt.Errorf("udev tags not canonical")
		}
		seen := map[string]bool{}
		for _, tag := range strings.Split(tags[1:len(tags)-1], ":") {
			if tag == "" || seen[tag] {
				return nil, fmt.Errorf("udev tag duplicate/empty")
			}
			seen[tag] = true
			h := normalOrdinaryUdevV2Hash(tag)
			bloom |= 1<<(h&63) | 1<<((h>>6)&63) | 1<<((h>>12)&63) | 1<<((h>>18)&63)
		}
	}
	binary.BigEndian.PutUint32(header[32:36], uint32(bloom>>32))
	binary.BigEndian.PutUint32(header[36:40], uint32(bloom))
	return append(header, raw...), nil
}

// 原Root nonce是32随机字节的64hex；kernel UUID只接受16字节/36字符。
// 独立取前128bit并设置RFC4122 version/variant，不把64hex尾串误作UUID。
func normalOrdinaryUdevV2NewTriggerUUID() (string, error) {
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return "", e
	}
	if len(nonce) != 64 || !physicalDigestValid(nonce) {
		return "", fmt.Errorf("original random nonce encoding differs")
	}
	return nonce[:8] + "-" + nonce[8:12] + "-4" + nonce[13:16] + "-a" + nonce[17:20] + "-" + nonce[20:32], nil
}
