---
theme: dashboard
sql:
  taxo: taxo_concentration.parquet
  biovol: biovolume_concentration.parquet
---

# Plankton data from Biogeochemical-Argo floats

## Data from [Argo GDAC](https://www.argodatamgt.org/DataAccess.html)

```js
const traj_argo = FileAttachment("trajectories.csv").csv({typed: true});
```

```js
const wmo = [...new Set(traj_argo.map(d => d.wmo))].sort((a, b) => a - b);

const taxo_classes = ["Acantharia", "Actinopterygii", "Appendicularia", "Aulacanthidae", "Calanoida", "Chaetognatha", "Collodaria", "Creseis", "Foraminifera", "Rhizaria", "Salpida", "artefact", "crystal", "detritus", "fiber", "other<living", "puff", "small-bell<Hydrozoa", "solitaryglobule", "tuff"];
const park_depths = [200, 500, 1000];
```

```js
const colorScale = d3.scaleOrdinal()
  .domain(wmo.map(String))
  .range(d3.schemeCategory10.concat(d3.schemeSet1, d3.schemeDark2));
```

```js
const pickTaxoClassInput = Inputs.select(taxo_classes, {label: "Taxonomic class", value: "detritus"});
const pickTaxoClass = Generators.input(pickTaxoClassInput);

const pickDepthInput = Inputs.checkbox(park_depths, {
  label: "Parking depth (m)",
  value: [1000]
});
const pickDepth = Generators.input(pickDepthInput);

const pickFloatInput = Inputs.select(wmo, {
  label: "Float WMO",
  multiple: true,
  value: [wmo[0]]
});
const sel = pickFloatInput.querySelector("select");
if (sel) { sel.size = 6; }
const pickFloat = Generators.input(pickFloatInput);
```

```js
const selectedWmos = Array.from(pickFloat);
```

```js
const pickDepthStr = pickDepth.length > 0 ? pickDepth.join(',') : 'NULL';
const pickFloatStr = selectedWmos.length > 0 ? selectedWmos.map(d => `'${d}'`).join(',') : 'NULL';

const taxo_filtered = (pickDepth.length === 0 || selectedWmos.length === 0)
  ? []
  : await sql([`
  SELECT park_depth, wmo, taxo_class, concentration, juld
  FROM taxo
  WHERE park_depth IN (${pickDepthStr})
    AND taxo_class = '${pickTaxoClass}'
    AND wmo IN (${pickFloatStr})
`]);

const biovol_filtered = (pickDepth.length === 0 || selectedWmos.length === 0)
  ? []
  : await sql([`
  SELECT park_depth, wmo, taxo_class, biovolume, juld
  FROM biovol
  WHERE park_depth IN (${pickDepthStr})
    AND taxo_class = '${pickTaxoClass}'
    AND wmo IN (${pickFloatStr})
`]);
```

```js
const taxoStats = (() => {
  const vals = [...taxo_filtered].map(d => d.concentration).filter(v => v != null).sort((a, b) => a - b);
  if (vals.length === 0) return {median: 20, max: 1000};
  return {
    median: Math.ceil(vals[Math.floor(vals.length / 2)] * 2),
    max: Math.ceil(d3.max(vals))
  };
})();

const maxTaxoInput = Inputs.range([0, taxoStats.max], {label: "Max Y-axis", step: 1, value: taxoStats.median});
const maxTaxo = Generators.input(maxTaxoInput);

const biovolStats = (() => {
  const vals = [...biovol_filtered].map(d => d.biovolume).filter(v => v != null).sort((a, b) => a - b);
  if (vals.length === 0) return {median: 20, max: 1000};
  return {
    median: Math.ceil(vals[Math.floor(vals.length / 2)] * 2),
    max: Math.ceil(d3.max(vals))
  };
})();

const maxBiovolInput = Inputs.range([0, biovolStats.max], {label: "Max Y-axis", step: 1, value: biovolStats.median});
const maxBiovol = Generators.input(maxBiovolInput);
```

```js
const taxo_plot = resize((width) => Plot.plot({
  marks: [
    Plot.dot(taxo_filtered, {
      y: "concentration", x: "juld",
      fill: d => colorScale(String(d.wmo)),
      r: 3, opacity: 0.7, symbol: "park_depth"
    }),
    Plot.tip(taxo_filtered, Plot.pointer({
      y: "concentration", x: "juld",
      title: d => `WMO: ${d.wmo}\nDepth: ${d.park_depth} m`
    })),
    Plot.crosshair(taxo_filtered, {x: "juld", y: "concentration"}),
  ],
  y: {label: "Concentration (#/L)", domain: [0, maxTaxo]},
  x: {label: "Date"},
  clip: true,
  color: {legend: true, domain: selectedWmos.map(String), range: selectedWmos.map(w => colorScale(String(w)))},
  width, height: 400,
  style: {fontFamily: "sans-serif", fontSize: 12}
}));
```

```js
const biovol_plot = resize((width) => Plot.plot({
  marks: [
    Plot.dot(biovol_filtered, {
      y: "biovolume", x: "juld",
      fill: d => colorScale(String(d.wmo)),
      r: 1, opacity: 0.5, symbol: "park_depth"
    }),
    Plot.tip(biovol_filtered, Plot.pointer({
      y: "biovolume", x: "juld",
      title: d => `WMO: ${d.wmo}\nDepth: ${d.park_depth} m`
    })),
    Plot.crosshair(biovol_filtered, {x: "juld", y: "biovolume"}),
  ],
  y: {label: "Biovolume (µm³/mL)", domain: [0, maxBiovol]},
  x: {label: "Date"},
  clip: true,
  color: {legend: true, domain: selectedWmos.map(String), range: selectedWmos.map(w => colorScale(String(w)))},
  width, height: 400,
  style: {fontFamily: "sans-serif", fontSize: 12}
}));
```

<div class="card">
  ${pickDepthInput}
  ${pickFloatInput}
</div>

<div class="card">
  ${pickTaxoClassInput}
</div>

<div class="grid grid-cols-2">
  <div class="card">
    <h2>Taxonomic concentrations at parking depth</h2>
    <h3>Classification from the <a href="https://github.com/ecotaxa/uvpec">UVPec</a> algorithm.</h3>
    ${maxTaxoInput}
    ${taxo_plot}
  </div>
  <div class="card">
    <h2>Biovolume at parking depth</h2>
    <h3>Classification from the <a href="https://github.com/ecotaxa/uvpec">UVPec</a> algorithm.</h3>
    ${maxBiovolInput}
    ${biovol_plot}
  </div>
</div>

<div class="small note">
  The UVP6 is an underwater imaging system that measures the size and gray level of marine particles, with an <a href='https://github.com/ecotaxa/uvpec'>integrated classification algorithm</a>.<br><br>
  Data from the <a href="https://argo.ucsd.edu">International Argo Program</a>.
</div>
