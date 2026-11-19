package main

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

type analyticsPoint struct {
	Time  string `json:"time"`
	Count int    `json:"count"`
}

type analyticsSeries struct {
	HazardType string           `json:"hazard_type"`
	Points     []analyticsPoint `json:"points"`
}

type analyticsCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type analyticsResponse struct {
	From             string            `json:"from"`
	To               string            `json:"to"`
	Partial          bool              `json:"partial"`
	UnavailableUnits []string          `json:"unavailable_units"`
	Series           []analyticsSeries `json:"series"`
	Severity         []analyticsCount  `json:"severity"`
	Areas            []analyticsCount  `json:"areas"`
}

func (s *server) analyticsPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(analyticsHTML))
}

// analyticsSummary reads through the same Citus cursor path as the Audit API,
// then aggregates in the process that owns canonical storage. This keeps
// dashboard numbers aligned with /internal/audit/events and preserves the
// partial marker when a worker is unreachable.
func (s *server) analyticsSummary(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now
	if raw := r.URL.Query().Get("from"); raw != "" {
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "from must be RFC3339"}})
			return
		}
		from = value.UTC()
	}
	if raw := r.URL.Query().Get("to"); raw != "" {
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "to must be RFC3339"}})
			return
		}
		to = value.UTC()
	}
	if !to.After(from) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "to must be after from"}})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	source := r.URL.Query().Get("source")
	events := []canonical.Event{}
	units := []string{}
	seenUnits := map[string]bool{}
	partial := false
	cursor := ""
	for {
		page, err := s.store.Query(ctx, canonical.Query{
			Source: source, Since: from.Format(time.RFC3339Nano), Limit: 1000, Cursor: cursor,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		for _, event := range page.Items {
			if event.IngestedAt.After(to) {
				continue
			}
			events = append(events, event)
		}
		if page.Partial {
			partial = true
		}
		for _, unit := range page.UnavailableUnits {
			if !seenUnits[unit] {
				seenUnits[unit] = true
				units = append(units, unit)
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}

	type seriesBuilder struct {
		Points map[string]int
	}
	seriesMap := map[string]*seriesBuilder{}
	severityMap := map[string]int{}
	areaMap := map[string]int{}
	for _, event := range events {
		key := event.IngestedAt.UTC().Truncate(time.Hour).Format(time.RFC3339)
		builder := seriesMap[event.HazardType]
		if builder == nil {
			builder = &seriesBuilder{Points: map[string]int{}}
			seriesMap[event.HazardType] = builder
		}
		builder.Points[key]++
		severityMap[event.Severity]++
		areaMap[event.AreaName]++
	}

	series := make([]analyticsSeries, 0, len(seriesMap))
	for hazard, builder := range seriesMap {
		points := make([]analyticsPoint, 0, len(builder.Points))
		for stamp, count := range builder.Points {
			points = append(points, analyticsPoint{Time: stamp, Count: count})
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		series = append(series, analyticsSeries{HazardType: hazard, Points: points})
	}
	sort.Slice(series, func(i, j int) bool { return series[i].HazardType < series[j].HazardType })
	sort.Strings(units)

	writeJSON(w, http.StatusOK, analyticsResponse{
		From:             from.Format(time.RFC3339),
		To:               to.Format(time.RFC3339),
		Partial:          partial,
		UnavailableUnits: units,
		Series:           series,
		Severity:         sortedCounts(severityMap),
		Areas:            sortedCounts(areaMap),
	})
}

func sortedCounts(values map[string]int) []analyticsCount {
	result := make([]analyticsCount, 0, len(values))
	for name, count := range values {
		result = append(result, analyticsCount{Name: name, Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count == result[j].Count {
			return result[i].Name < result[j].Name
		}
		return result[i].Count > result[j].Count
	})
	return result
}

const analyticsHTML = `<!doctype html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BNPB — Analitik HazardEvent</title>
<style>
body{font-family:system-ui,sans-serif;margin:24px;background:#faf7f2;color:#20211f}
h1{font-size:1.4rem;margin:0 0 4px}
#status{padding:8px 12px;border-radius:6px;background:#e8f3e8;color:#245c2b;display:inline-block}
#status.partial{background:#fff0d6;color:#7a4b00}
#chart{width:100%;max-width:1100px;height:320px;background:white;border:1px solid #ddd;border-radius:8px}
.row{display:flex;gap:24px;flex-wrap:wrap;margin-top:20px}
.card{background:white;border:1px solid #ddd;border-radius:8px;padding:16px;min-width:300px;flex:1}
table{border-collapse:collapse;width:100%}
td,th{text-align:left;padding:4px 8px;border-bottom:1px solid #eee}
th{font-size:.8rem;color:#666}
label{margin-right:8px}
button{padding:6px 12px;border:1px solid #bbb;border-radius:5px;background:white;cursor:pointer}
</style>
</head>
<body>
<h1>BNPB — Analitik HazardEvent</h1>
<p>Agregasi membaca jalur audit Citus yang sama dengan <code>/internal/audit/events</code>.</p>
<div>
  <label>Dari <input id="from" type="datetime-local"></label>
  <label>Sampai <input id="to" type="datetime-local"></label>
  <button id="load">Muat</button>
</div>
<p><span id="status">Memuat…</span></p>
<canvas id="chart" width="1100" height="320"></canvas>
<div class="row">
  <div class="card"><h2>Per severity</h2><table id="severity"><tbody></tbody></table></div>
  <div class="card"><h2>Per area_name</h2><table id="areas"><tbody></tbody></table></div>
</div>
<script>
const chart = document.getElementById('chart');
const status = document.getElementById('status');
const colors = ['#d24b35','#20639b','#3caea3','#f6b93b','#8e44ad','#2e86de'];
function isoLocal(d){ return d.toISOString().slice(0,16); }
function setInputs(){
  const now = new Date();
  const from = new Date(now.getTime() - 24*60*60*1000);
  document.getElementById('from').value = isoLocal(from);
  document.getElementById('to').value = isoLocal(now);
}
function renderTable(id, rows){
  const body = document.querySelector('#' + id + ' tbody');
  body.innerHTML = '';
  if(!rows.length){ const tr=document.createElement('tr'); const td=document.createElement('td'); td.textContent='Tidak ada data'; tr.appendChild(td); body.appendChild(tr); return; }
  for(const row of rows){
    const tr=document.createElement('tr');
    const a=document.createElement('td'); a.textContent=row.name || '(kosong)';
    const b=document.createElement('td'); b.textContent=row.count;
    tr.appendChild(a); tr.appendChild(b); body.appendChild(tr);
  }
}
function draw(data){
  const ctx = chart.getContext('2d');
  const W = chart.width, H = chart.height, L=50, R=20, T=20, B=40;
  ctx.clearRect(0,0,W,H);
  const times = new Set();
  for(const s of data.series) for(const p of s.points) times.add(p.time);
  const labels = Array.from(times).sort();
  const values = data.series.flatMap(s => s.points.map(p => p.count));
  const max = Math.max(1, ...values);
  ctx.strokeStyle='#ddd'; ctx.beginPath();
  for(let i=0;i<=4;i++){ const y=T+(H-T-B)*i/4; ctx.moveTo(L,y); ctx.lineTo(W-R,y); }
  ctx.stroke();
  ctx.fillStyle='#555'; ctx.font='12px system-ui';
  for(let i=0;i<=4;i++){ const y=T+(H-T-B)*i/4; ctx.fillText(String(Math.round(max*(4-i)/4)), 8, y+4); }
  if(!labels.length){ ctx.fillText('Tidak ada data pada rentang ini', L, H/2); return; }
  labels.forEach((label, i) => { if(labels.length < 8 || i % Math.ceil(labels.length/8) === 0){ const x=L+(W-L-R)*i/Math.max(1,labels.length-1); ctx.save(); ctx.translate(x,H-15); ctx.rotate(-0.35); ctx.fillText(label.slice(5,16),0,0); ctx.restore(); } });
  data.series.forEach((s, si) => {
    ctx.strokeStyle = colors[si % colors.length]; ctx.lineWidth = 2; ctx.beginPath();
    s.points.forEach((p, pi) => {
      const xi = labels.indexOf(p.time);
      const x = L+(W-L-R)*xi/Math.max(1,labels.length-1);
      const y = T+(H-T-B)*(1-p.count/max);
      if(pi===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    });
    ctx.stroke();
    ctx.fillStyle = colors[si % colors.length];
    ctx.fillRect(L+si*150, 8, 10, 10);
    ctx.fillStyle='#333'; ctx.fillText(s.hazard_type, L+14+si*150, 18);
  });
}
async function load(){
  status.className='';
  const from = new Date(document.getElementById('from').value);
  const to = new Date(document.getElementById('to').value);
  if(isNaN(from) || isNaN(to)){ status.textContent='Rentang waktu tidak valid'; status.className='partial'; return; }
  status.textContent='Memuat…';
  try{
    const response = await fetch('/internal/analytics/summary?from=' + encodeURIComponent(from.toISOString()) + '&to=' + encodeURIComponent(to.toISOString()));
    const data = await response.json();
    if(!response.ok) throw new Error(data.error ? data.error.message : 'request failed');
    status.textContent = data.partial ? ('Data belum lengkap: ' + data.unavailable_units.join(', ')) : 'Data lengkap';
    status.className = data.partial ? 'partial' : '';
    draw(data); renderTable('severity', data.severity); renderTable('areas', data.areas);
  }catch(error){
    status.textContent = 'Gagal memuat: ' + error.message; status.className='partial';
  }
}
setInputs(); document.getElementById('load').addEventListener('click', load); load();
</script>
</body>
</html>`
