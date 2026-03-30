---
theme: dashboard
sql:
  particle: particle_concentrations.parquet
  pss: particle_size_spectra.parquet
  ost: optical_sediment_trap.parquet
---

# Particles data from Biogeochemical-Argo floats

## Data from [Argo GDAC](https://www.argodatamgt.org/DataAccess.html)

```js
const traj_argo = FileAttachment("trajectories.csv").csv({typed: true});
```

```js
const wmo = [...new Set(traj_argo.map(d => d.wmo))].sort((a, b) => a - b);

const zones_result = await sql([`SELECT DISTINCT zone FROM particle WHERE zone IS NOT NULL ORDER BY zone`]);
const zones = [...zones_result].map(d => d.zone);

const lpm_classes = [50.8, 64, 80.6, 102, 128, 161, 203, 256, 323, 406, 512, 645, 813, 1020, 1290, 1630, 2050, 2580];
const park_depths = [200, 500, 1000];
```

```js
const colorScale = d3.scaleOrdinal()
  .domain(wmo.map(String))
  .range(d3.schemeCategory10.concat(d3.schemeSet1, d3.schemeDark2));

const zoneColorScale = d3.scaleOrdinal()
  .domain(zones)
  .range(d3.schemeTableau10.concat(["#aec7e8"]));
```

```js
const pickSizeClass = view(Inputs.select(lpm_classes, {
  label: "Size class (um)",
  value: 102
}));

const pickDepth = view(Inputs.checkbox(park_depths, {
  label: "Parking depth (m)",
  value: [1000]
}));

const pickFloat = view(Inputs.select(wmo, {
  label: "Float WMO",
  multiple: true,
  value: [wmo[0]]
}));

const colorByRegion = view(Inputs.toggle({
  label: "Colour by region",
  value: false
}));

```

```js
const selectedWmos = Array.from(pickFloat);
```

```js
// SQL queries
const pickDepthStr = pickDepth.length > 0 ? pickDepth.join(',') : 'NULL';
const pickFloatStr = selectedWmos.length > 0 ? selectedWmos.map(d => `'${d}'`).join(',') : 'NULL';

const particle_filtered = await sql([`
  SELECT park_depth, wmo, size, concentration, juld, zone
  FROM particle
  WHERE park_depth IN (${pickDepthStr})
    AND size = ${pickSizeClass}
    AND wmo IN (${pickFloatStr})
`]);

const ost_filtered = await sql([`
  SELECT *
  FROM ost
  WHERE park_depth IN (${pickDepthStr})
    AND wmo IN (${pickFloatStr})
`]);

const pss_filtered = await sql([`
  SELECT *
  FROM pss
  WHERE park_depth IN (${pickDepthStr})
    AND wmo IN (${pickFloatStr})
`]);
```

```js
const nObservations = [...particle_filtered].length;
const dateExtent = d3.extent(particle_filtered, d => d.juld);
```


```js
// Leaflet map: all floats shown, click to toggle selection
const mapDiv = (() => {
  const div = document.createElement("div");
  div.style.height = "400px";
  div.style.width = "100%";

  const selected = new Set(selectedWmos);
  const groupedData = d3.group(traj_argo, d => d.wmo);
  const map = L.map(div).setView([0, 0], 2);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    maxZoom: 20
  }).addTo(map);

  const allPolylines = [];

  groupedData.forEach((floatData, wmoKey) => {
    floatData.sort((a, b) => a.cycle - b.cycle);
    const latlngs = floatData.map(d => [d.latitude, d.longitude]);
    const isSelected = selected.has(wmoKey);
    const baseColor = isSelected ? colorScale(String(wmoKey)) : "#F0F0F0";
    const baseWeight = isSelected ? 4 : 2;
    const baseOpacity = isSelected ? 0.9 : 0.5;

    const polyline = L.polyline(latlngs, {
      color: baseColor,
      weight: baseWeight,
      opacity: baseOpacity
    }).addTo(map);

    polyline.bindTooltip(`WMO: ${wmoKey}${isSelected ? " ✓" : ""}`, {permanent: false, direction: 'top', opacity: 0.8});

    polyline.on('mouseover', function () {
      this.setStyle({color: '#F0F0F0', weight: 5});
      this.openTooltip();
    });
    polyline.on('mouseout', function () {
      this.setStyle({color: baseColor, weight: baseWeight, opacity: baseOpacity});
      this.closeTooltip();
    });

    allPolylines.push(polyline);

    if (latlngs.length > 0) {
      const marker = L.circleMarker(latlngs[latlngs.length - 1], {
        color: isSelected ? "#B33951" : "#666",
        fillColor: isSelected ? "black" : "#333",
        fillOpacity: 0.5,
        radius: isSelected ? 5 : 3
      }).addTo(map);
      marker.bindPopup(`Float: ${wmoKey}<br>Last update: ${floatData[floatData.length - 1].date}`);
    }
  });

  if (allPolylines.length > 0) {
    const group = L.featureGroup(allPolylines);
    map.fitBounds(group.getBounds(), {padding: [30, 30], maxZoom: 1});
  }

  requestAnimationFrame(() => map.invalidateSize());
  return div;
})();
```

```js
const maxConcentrationInput = Inputs.range([0, 10000], {label: "Max Y-axis", step: 1, value: 20});
const maxConcentration = Generators.input(maxConcentrationInput);

const maxFluxInput = Inputs.range([0, 1500], {label: "Max Y-axis", step: 10, value: 200});
const maxFlux = Generators.input(maxFluxInput);
```

```js
const particle_plot = resize((width) => Plot.plot({
  marks: [
    Plot.dot(particle_filtered, {
      y: "concentration", x: "juld",
      fill: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      r: 1, opacity: 0.5
    }),
    Plot.tip(particle_filtered, Plot.pointer({
      y: "concentration", x: "juld",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nDepth: ${d.park_depth.toFixed(0)} m`
    })),
    Plot.crosshair(particle_filtered, {x: "juld", y: "concentration"}),
    Plot.lineY(particle_filtered, Plot.windowY({
      k: 60, reduce: "median", x: "juld", y: "concentration",
      stroke: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      strokeWidth: 3, z: d => `${d.wmo}-${d.park_depth}`
    }), {sort: "juld"})
  ],
  y: {label: "Concentration (#/L)", domain: [0, maxConcentration]},
  x: {label: "Date"},
  clip: true,
  color: colorByRegion
    ? {legend: true, domain: zones, range: zoneColorScale.range()}
    : {legend: true, domain: selectedWmos.map(String), range: selectedWmos.map(w => colorScale(String(w)))},
  width, height: 400,
  style: {fontFamily: "sans-serif", fontSize: 12}
}));
```

```js
const pss_plot = resize((width) => Plot.plot({
  marks: [
    Plot.dot(pss_filtered, {
      y: "mean_slope", x: "juld_date",
      fill: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      r: 3, opacity: 0.5, symbol: "park_depth"
    }),
    Plot.tip(pss_filtered, Plot.pointer({
      y: "mean_slope", x: "juld_date",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nDepth: ${d.park_depth} m`
    })),
    Plot.lineY(pss_filtered, Plot.windowY({
      k: 12, reduce: "median", x: "juld_date", y: "mean_slope",
      stroke: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      strokeWidth: 3, z: d => `${d.wmo}-${d.park_depth}`
    })),
    Plot.crosshair(pss_filtered, {x: "juld_date", y: "mean_slope"})
  ],
  y: {label: "Mean slope"},
  x: {label: "Date", type: "utc",
    tickFormat: d => {
      const date = new Date(d);
      return date.getUTCMonth() === 0 ? d3.utcFormat("Jan\n%Y")(date) : d3.utcFormat("%b")(date);
    }
  },
  color: colorByRegion
    ? {legend: true, domain: zones, range: zoneColorScale.range()}
    : {legend: true, domain: selectedWmos.map(String), range: selectedWmos.map(w => colorScale(String(w)))},
  width, height: 400,
  style: {fontFamily: "sans-serif", fontSize: 12}
}));
```

```js
const ost_plot = resize((width) => Plot.plot({
  marks: [
    Plot.dot(ost_filtered, {
      y: "total_flux", x: "max_time",
      fill: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      r: 3, opacity: 0.5, symbol: "park_depth"
    }),
    Plot.tip(ost_filtered, Plot.pointer({
      y: "total_flux", x: "max_time",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nDepth: ${d.park_depth} m\nSmall: ${d.small_flux.toFixed(2)}\nLarge: ${d.large_flux.toFixed(2)}`
    })),
    Plot.lineY(ost_filtered, Plot.windowY({
      k: 12, reduce: "median", x: "max_time", y: "total_flux",
      stroke: d => colorByRegion ? zoneColorScale(d.zone) : colorScale(String(d.wmo)),
      strokeWidth: 3, z: d => `${d.wmo}-${d.park_depth}`
    })),
    Plot.crosshair(ost_filtered, {x: "max_time", y: "total_flux"})
  ],
  y: {label: "Total particle flux (mg C m⁻² d⁻¹)", domain: [0, maxFlux]},
  x: {label: "Date"},
  clip: true,
  color: colorByRegion
    ? {legend: true, domain: zones, range: zoneColorScale.range()}
    : {legend: true, domain: selectedWmos.map(String), range: selectedWmos.map(w => colorScale(String(w)))},
  width, height: 400,
  style: {fontFamily: "sans-serif", fontSize: 12}
}));
```

<div class="grid grid-cols-2">
  <div class="card">
    <h2>Observations</h2>
    <span class="big">${nObservations.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Date range</h2>
    <span class="big">${dateExtent[0] ? d3.utcFormat("%b %Y")(dateExtent[0]) : "---"} - ${dateExtent[1] ? d3.utcFormat("%b %Y")(dateExtent[1]) : "---"}</span>
  </div>
</div>

<div class="grid grid-cols-2">
  <div class="card" style="padding: 0;">
    <div style="padding: 1rem;">
      <h2>Float trajectories</h2>
      <h3>Red markers show the last known position</h3>
      ${mapDiv}
    </div>
  </div>
  <div class="card">
    <h2>Particle size spectra</h2>
    <h3>A very negative slope means concentration drops rapidly with particle size.</h3>
    ${pss_plot}
  </div>
</div>

<div class="grid grid-cols-2">
  <div class="card">
    <h2>Particle concentrations at parking depth</h2>
    <h3>Measured with the <a href="http://www.hydroptic.com/index.php/public/Page/product_item/UVP6-LP">UVP6</a>.</h3>
    ${maxConcentrationInput}
    ${particle_plot}
  </div>
  <div class="card">
    <h2>Total carbon flux (optical sediment trap)</h2>
    <h3>Following <a href='https://doi.org/10.1029/2022GB007624'>Terrats et al. (2023)</a></h3>
    ${maxFluxInput}
    ${ost_plot}
  </div>
</div>

<div class="small note">
  The UVP6 is an underwater imaging system that measures the size and gray level of marine particles, with an <a href='https://github.com/ecotaxa/uvpec'>integrated classification algorithm</a>.<br><br>
  The transmissometer, mounted vertically on autonomous floats, measures particle accumulation on the upward-facing optical window at parking depth, functioning as an optical sediment trap (OST).<br><br>
  Moving median smoothing: k=60 for particle concentrations, k=12 for OST and particle size spectra. Outliers removed via the <a href='https://en.wikipedia.org/wiki/Interquartile_range#Outliers'>IQR method</a>.<br><br>
  Data from the <a href="https://argo.ucsd.edu">International Argo Program</a>.
</div>
