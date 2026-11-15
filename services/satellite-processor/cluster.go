package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type feedEvent struct {
	Offset     int64
	HasOffset  bool
	Type       string
	Data       json.RawMessage
	ReceivedAt time.Time
	Flush      bool
}
type detection struct {
	Key        string    `json:"key"`
	Satellite  string    `json:"satellite"`
	AcqDate    string    `json:"acq_date"`
	AcqTime    string    `json:"acq_time"`
	Latitude   float64   `json:"latitude"`
	Longitude  float64   `json:"longitude"`
	Confidence string    `json:"confidence"`
	FRP        float64   `json:"frp"`
	EventTime  time.Time `json:"event_time"`
}
type feedMutation struct {
	Detection *detection
	TargetKey string
	Remove    bool
	Changes   map[string]json.RawMessage
}
type province struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func parseFeedMutation(kind string, raw []byte, offset int64) (feedMutation, error) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "heartbeat" {
		return feedMutation{}, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return feedMutation{}, fmt.Errorf("decode SSE %s JSON: %w", kind, err)
	}
	payload := root
	if nested := root["detection"]; len(nested) > 0 {
		_ = json.Unmarshal(nested, &payload)
	} else if nested := root["data"]; len(nested) > 0 {
		var m map[string]json.RawMessage
		if json.Unmarshal(nested, &m) == nil && m != nil {
			payload = m
		}
	}
	target := ""
	for _, name := range []string{"detection_key", "target_key", "previous_key", "retract_key", "key"} {
		if v := root[name]; len(v) > 0 {
			target = rawString(v)
			break
		}
		if v := payload[name]; len(v) > 0 {
			target = rawString(v)
			break
		}
	}
	if prior := root["previous"]; len(prior) > 0 && target == "" {
		if m, err := parseDetection(prior); err == nil {
			target = m.Key
		}
	}
	if kind == "retract" {
		if target == "" {
			if len(root["detection"]) > 0 {
				if d, err := parseDetection(root["detection"]); err == nil {
					target = d.Key
				}
			}
			if target == "" {
				if d, err := parseDetection(raw); err == nil {
					target = d.Key
				}
			}
		}
		if target == "" {
			return feedMutation{}, fmt.Errorf("retract at offset %d lacks target detection key", offset)
		}
		return feedMutation{TargetKey: target, Remove: true}, nil
	}
	if kind != "detection" && kind != "update" {
		return feedMutation{}, fmt.Errorf("unsupported SSE event type %q", kind)
	}
	if kind == "update" && len(root["changes"]) > 0 {
		if target == "" {
			return feedMutation{}, fmt.Errorf("update at offset %d lacks target detection key", offset)
		}
		var changes map[string]json.RawMessage
		if err := json.Unmarshal(root["changes"], &changes); err != nil || len(changes) == 0 {
			return feedMutation{}, fmt.Errorf("update at offset %d has invalid changes", offset)
		}
		return feedMutation{TargetKey: target, Changes: changes}, nil
	}
	d, err := parseDetectionMap(payload)
	if err != nil { // Some feeds wrap data with metadata; try the complete envelope as a compatibility path.
		d, err = parseDetection(raw)
	}
	if err != nil {
		return feedMutation{}, fmt.Errorf("invalid %s detection at offset %d: %w", kind, offset, err)
	}
	return feedMutation{Detection: &d, TargetKey: target}, nil
}

func parseDetection(raw []byte) (detection, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return detection{}, err
	}
	return parseDetectionMap(m)
}
func parseDetectionMap(m map[string]json.RawMessage) (detection, error) {
	var d detection
	d.Satellite = rawString(m["satellite"])
	d.AcqDate = rawString(m["acq_date"])
	d.AcqTime = rawString(m["acq_time"])
	if d.Satellite == "" || d.AcqDate == "" || d.AcqTime == "" {
		return d, fmt.Errorf("satellite, acq_date, and acq_time are required")
	}
	latRaw, lonRaw := m["latitude"], m["longitude"]
	var err error
	if d.Latitude, err = rawFloat(latRaw); err != nil {
		return d, fmt.Errorf("latitude: %w", err)
	}
	if d.Longitude, err = rawFloat(lonRaw); err != nil {
		return d, fmt.Errorf("longitude: %w", err)
	}
	if math.IsNaN(d.Latitude) || math.IsInf(d.Latitude, 0) || d.Latitude < -90 || d.Latitude > 90 || math.IsNaN(d.Longitude) || math.IsInf(d.Longitude, 0) || d.Longitude < -180 || d.Longitude > 180 {
		return d, fmt.Errorf("coordinates out of range")
	}
	d.Confidence = strings.ToLower(rawString(m["confidence"]))
	if d.Confidence == "" {
		return d, fmt.Errorf("confidence is required")
	}
	if v := m["frp"]; len(v) > 0 {
		d.FRP, err = rawFloat(v)
		if err != nil || math.IsNaN(d.FRP) || math.IsInf(d.FRP, 0) || d.FRP < 0 {
			return d, fmt.Errorf("invalid frp")
		}
	}
	d.EventTime, err = parseAcquisitionTime(d.AcqDate, d.AcqTime)
	if err != nil {
		return d, err
	}
	d.Key = strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime, rawString(latRaw), rawString(lonRaw)}, "|")
	return d, nil
}
func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}
func rawFloat(raw json.RawMessage) (float64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing number")
	}
	return strconv.ParseFloat(rawString(raw), 64)
}
func parseAcquisitionTime(date, acq string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid acq_date %q", date)
	}
	clock := strings.TrimSpace(acq)
	if strings.Contains(clock, ":") {
		parts := strings.Split(clock, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
		}
		hour, e := strconv.Atoi(parts[0])
		if e != nil {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
		}
		minute, e := strconv.Atoi(parts[1])
		if e != nil {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
		}
		second := 0
		if len(parts) == 3 {
			second, e = strconv.Atoi(parts[2])
			if e != nil {
				return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
			}
		}
		if hour > 23 || minute > 59 || second > 59 {
			return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
		}
		return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, second, 0, time.UTC), nil
	}
	clock = strings.TrimLeft(clock, "+")
	clock = strings.TrimSpace(clock)
	if len(clock) > 4 {
		return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
	}
	for len(clock) < 4 {
		clock = "0" + clock
	}
	hour, err := strconv.Atoi(clock[:2])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
	}
	minute, err := strconv.Atoi(clock[2:])
	if err != nil || hour > 23 || minute > 59 {
		return time.Time{}, fmt.Errorf("invalid acq_time %q", acq)
	}
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.UTC), nil
}

func clusterDetections(all []detection) ([][]detection, int64) {
	eligible := make([]detection, 0, len(all))
	var singletons int64
	for _, d := range all {
		if strings.EqualFold(d.Confidence, "low") || d.Confidence == "l" {
			continue
		}
		eligible = append(eligible, d)
	}
	sortDetections(eligible)
	n := len(eligible)
	parent := make([]int, n)
	rank := make([]uint8, n)
	for i := range parent {
		parent[i] = i
	}
	find := func(x int) int { return x }
	_ = find
	var root func(int) int
	root = func(x int) int {
		if parent[x] != x {
			parent[x] = root(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := root(a), root(b)
		if ra == rb {
			return
		}
		if rank[ra] < rank[rb] {
			ra, rb = rb, ra
		}
		parent[rb] = ra
		if rank[ra] == rank[rb] {
			rank[ra]++
		}
	}
	const cell = 1.0 / 111.195
	type cellKey struct{ lat, lon int }
	buckets := map[cellKey][]int{}
	active := make([]bool, n)
	left := 0
	lonCells := int(math.Ceil(360 / cell))
	for i, d := range eligible {
		for left < i && d.EventTime.Sub(eligible[left].EventTime) > 6*time.Hour {
			old := eligible[left]
			k := cellKey{int(math.Floor((old.Latitude + 90) / cell)), int(math.Floor((old.Longitude + 180) / cell))}
			list := buckets[k]
			for j, v := range list {
				if v == left {
					buckets[k] = append(list[:j], list[j+1:]...)
					break
				}
			}
			active[left] = false
			left++
		}
		latRadius := int(math.Ceil((1.0/111.195)/cell)) + 1
		cos := math.Abs(math.Cos(d.Latitude * math.Pi / 180))
		if cos < 0.0001 {
			cos = 0.0001
		}
		lonRadius := int(math.Ceil((1.0/(111.195*cos))/cell)) + 1
		if lonRadius > lonCells/2 {
			lonRadius = lonCells / 2
		}
		latCell := int(math.Floor((d.Latitude + 90) / cell))
		lonCell := int(math.Floor((d.Longitude + 180) / cell))
		for dy := -latRadius; dy <= latRadius; dy++ {
			if latCell+dy < 0 || latCell+dy >= int(math.Ceil(180/cell)) {
				continue
			}
			for dx := -lonRadius; dx <= lonRadius; dx++ {
				x := (lonCell + dx) % lonCells
				if x < 0 {
					x += lonCells
				}
				for _, j := range buckets[cellKey{latCell + dy, x}] {
					if !active[j] || d.EventTime.Sub(eligible[j].EventTime) > 6*time.Hour {
						continue
					}
					if haversineKM(d.Latitude, d.Longitude, eligible[j].Latitude, eligible[j].Longitude) <= 1.0 {
						union(i, j)
					}
				}
			}
		}
		k := cellKey{latCell, lonCell}
		buckets[k] = append(buckets[k], i)
		active[i] = true
	}
	groups := map[int][]detection{}
	for i, d := range eligible {
		r := root(i)
		groups[r] = append(groups[r], d)
	}
	components := make([][]detection, 0, len(groups))
	for _, g := range groups {
		components = append(components, g)
	}
	sort.Slice(components, func(i, j int) bool { return components[i][0].Key < components[j][0].Key })
	for _, g := range components {
		if len(g) == 1 {
			singletons++
		}
	}
	return components, singletons
}
func sortDetections(ds []detection) {
	sort.Slice(ds, func(i, j int) bool {
		if ds[i].EventTime.Equal(ds[j].EventTime) {
			return ds[i].Key < ds[j].Key
		}
		return ds[i].EventTime.Before(ds[j].EventTime)
	})
}
func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.0088
	rad := math.Pi / 180
	dlat := (lat2 - lat1) * rad
	dlon := (lon2 - lon1) * rad
	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dlon/2)*math.Sin(dlon/2)
	return 2 * r * math.Asin(math.Sqrt(math.Min(1, a)))
}

func stableFireID(anchor string) string {
	sum := sha256.Sum256([]byte(anchor))
	return "fire-" + hex.EncodeToString(sum[:12])
}
func sha256Hex(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func buildWildfire(ds []detection) map[string]any {
	lat, lon := 0.0, 0.0
	earliest := ds[0].EventTime
	frp := 0.0
	maxConf := "low"
	satSet := map[string]bool{}
	keys := make([]string, 0, len(ds))
	for _, d := range ds {
		lat += d.Latitude
		lon += d.Longitude
		frp += d.FRP
		if d.EventTime.Before(earliest) {
			earliest = d.EventTime
		}
		satSet[d.Satellite] = true
		if confidenceRank(d.Confidence) > confidenceRank(maxConf) {
			maxConf = d.Confidence
		}
		keys = append(keys, d.Key)
	}
	lat /= float64(len(ds))
	lon /= float64(len(ds))
	satellites := make([]string, 0, len(satSet))
	for s := range satSet {
		satellites = append(satellites, s)
	}
	sort.Strings(satellites)
	sort.Strings(keys)
	severity := "WASPADA"
	if len(ds) >= 20 || frp >= 100 {
		severity = "AWAS"
	} else if len(ds) >= 6 || frp >= 30 {
		severity = "SIAGA"
	}
	area := nearestProvince(lat, lon, loadProvinceCentroids())
	return map[string]any{"hazard_type": "WILDFIRE", "severity": severity, "area_name": area, "latitude": lat, "longitude": lon, "occurred_at": earliest.UTC().Format(time.RFC3339), "attributes": map[string]any{"detection_count": len(ds), "satellites": satellites, "max_confidence": maxConf, "total_frp_mw": math.Round(frp*100) / 100, "detection_keys": keys}}
}
func confidenceRank(v string) int {
	switch strings.ToLower(v) {
	case "h", "high":
		return 3
	case "n", "nominal", "medium":
		return 2
	case "l", "low":
		return 1
	}
	return 0
}
func loadProvinceCentroids() []province {
	var p []province
	if len(provinceData) > 0 {
		_ = json.Unmarshal(provinceData, &p)
	}
	return p
}
func nearestProvince(lat, lon float64, ps []province) string {
	best := math.Inf(1)
	name := ""
	for _, p := range ps {
		d := haversineKM(lat, lon, p.Latitude, p.Longitude)
		if d < best {
			best = d
			name = p.Name
		}
	}
	return name
}

var provinceData []byte
