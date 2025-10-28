---
theme: dashboard
sql: 
  particle: particle_concentrations.parquet
  pss: particle_size_spectra.parquet
---

# Particles data from Biogeochemical-Argo floats

## Data from [Argo GDAC](http://www.argodatamgt.org/Access-to-data/Argo-GDAC-ftp-https-and-s3-servers)
```js
// Load the required libraries for leaflet map, see here https://observablehq.com/framework/imports
//import { scaleLinear } from 'npm:d3-scale';
//import { interpolateViridis } from 'npm:d3-scale-chromatic';
```

```js
//
// Load data snapshots (big datasets are loaded with SQL (and duckDB behind the scenes, see the specifications at the top of the file))
//

// changed my mind with this resource https://observablehq.com/framework/sql for the big dataset (not the trajectory one for the leaflet map)
// Particle data
//const argo = FileAttachment("LPM_data.parquet").parquet(); // need to rerun when the file changes (won't work with the sql header only)

// Trajectory data
const traj_argo = FileAttachment("trajectories.csv").csv({typed: true});

// Size spectra data
//const size_spectra = FileAttachment("size_spectra.parquet").parquet();

// OST data
//const ost_data = FileAttachment("optical_sediment_trap.parquet").parquet();

// Taxo data
//const taxo_data = FileAttachment("taxo_data.parquet").parquet();
```

```js
// declaration of key variables

// particle size classes
const lpm_classes = [50.8, 64, 80.6, 102, 128, 161, 203, 256, 323, 406, 512, 645, 813, 1020, 1290, 1630, 2050, 2580]

// wmos (unique id for BGC-Argo floats)
const wmo = [1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 5906970, 6904240, 6904241, 6990503, 6990514, 7901028]

// parking depths
const park_depths = [200, 500, 1000]

// Define a custom color palette (colorblind-friendly)
const colorPalette = [
  "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
  "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
];

// oceanic zones
const zones = ["Labrador Sea", "East Kerguelen", "Guinea Dome", "Apero mission", "North Pacific Gyre", "South Pacific Gyre", "West Kerguelen",
"Nordic Seas", "Tropical Indian Ocean", "Arabian Sea"]
```

```js
//
// input declarations
//
// define user inputs
const pickSizeClass = view(
  Inputs.select(
    lpm_classes,
    {
      multiple: false,
      label: "Size class (µm)",
      unique: true,
      sort: false,
      value: 102
    }
  )
);

const pickDepth = view(
  Inputs.checkbox(
    park_depths,
    {
      multiple: true,
      label: "Parking depth (m)",
      unique: true,
      sort: false,
      value: [1000]
    }
  )
);

const pickFloat = view(
  Inputs.select(
   wmo,
    {
      multiple: 5,
      label: "Float WMO",
      unique: true,
      sort: false,
      value: [1902578]
    }
  )
);

const colorByRegion = view(
  Inputs.toggle({label: "Colour by region", value: false}
  ));
```

```js
const particle_filtered = await sql([`SELECT * park_depth, WMO, size, concentration, juld, zone
                                      FROM particle
                                      WHERE park_depth IN (${pickDepth.length > 0 ? pickDepth.join(',') : 'NULL'})
                                      AND size IN (${[pickSizeClass]}) 
                                      AND wmo IN (${pickFloat.length > 0 ? pickFloat.join(',') : 'NULL'})`])

const maxConcentration = d3.max(particle_filtered, d => d.concentration);

// const ost_filtered = await sql([`SELECT * 
//                                  FROM ost 
//                                  WHERE park_depth IN (${pickDepth.length > 0 ? pickDepth.join(',') : 'NULL'})
//                                  AND wmo IN (${pickFloat.length > 0 ? pickFloat.join(',') : 'NULL'})`])    
                                 
const pss_filtered = await sql([`SELECT *
                                 FROM pss
                                 WHERE park_depth IN (${pickDepth.length > 0 ? pickDepth.join(',') : 'NULL'})
                                 AND wmo IN (${pickFloat.length > 0 ? pickFloat.join(',') : 'NULL'})`])
```


```js
// Create a color scale based on unique WMO values
const colorScale = d3.scaleOrdinal()
  .domain(wmo)
  .range(colorPalette);
```

```js
// leaflet map to plot floats' trajectories
// Thanks claude.ai
const div = display(document.createElement("div"));
div.style = "height: 500px;";

const map = L.map(div)
  .setView([0, 180], 2); // centered on Greenwich, zoom level 2

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
  maxZoom: 20
}).addTo(map);

const groupedData = d3.group(traj_argo, d => d.wmo);

// Prepare an array to hold all polylines
let allPolylines = [];

// For each float (WMO)
groupedData.forEach((floatData, wmo) => {
  // Sort the data by cycle to ensure correct trajectory
  floatData.sort((a, b) => a.cycle - b.cycle);
  
  // Extract coordinates
  const latlngs = floatData.map(d => [d.latitude, d.longitude]);
  
  // Create a polyline for the trajectory
  const polyline = L.polyline(latlngs, {
    //color: colorScale(wmo),
    color: "#F0F0F0",
    weight: 3,
    opacity: 0.7
  }).addTo(map);

  // Add a tooltip with the WMO identifier
  polyline.bindTooltip(`WMO: ${wmo}`, {
    permanent: false,
    direction: 'top',
    opacity: 0.7
  });

  // Add hover effect
  polyline.on('mouseover', function(e) {
    this.setStyle({
      color: 'black',
      weight: 5
    });
    this.openTooltip();
  });
  polyline.on('mouseout', function(e) {
    this.setStyle({
      //color: colorScale(wmo),
      color: '#F0F0F0',
      weight: 5,
      opacity: 0.7
    });
    this.closeTooltip();
  });
  
  // Add to our array of all polylines
  allPolylines.push(polyline);
  
  // Add a marker for the last position
  L.circleMarker(latlngs[latlngs.length - 1], {
    //color: colorScale(wmo),
    color: "#B33951",
    fillColor: "black",
    fillOpacity: 0.5,
    radius: 2
  }).addTo(map)
    .bindPopup(`Float: ${wmo}<br>Last update on ${floatData[floatData.length - 1].date}`);
});

// Create a feature group from all polylines
const group = L.featureGroup(allPolylines);
```

```js
// Create the particle plot (when floats have reached their parking depth)
const particle_plot = Plot.plot({
  marks: [
    Plot.dot(particle_filtered, {
      y: "concentration",
      x: "juld",
      fill: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo), // Use the custom color scale
      r: 1,
      opacity: 0.5,
    }),
    Plot.tip(particle_filtered, Plot.pointer({
      y: "concentration",
      x: "juld",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nParking depth: ${d.park_depth} m`
    })),
    Plot.crosshair(particle_filtered, {x: "juld", y: "concentration"}),
    Plot.lineY(particle_filtered, Plot.windowY({
        k: 60,
        reduce: "median",
        x: "juld",
        y: "concentration",
        stroke: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo),
        strokeWidth: 3,
        // z: d => `${d.wmo}-${d.park_depth}`}), {sort: "juld"})
        z: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo)}))
  ],
  y: {
    label: "Concentration (#/L)",
    reverse: false
  },
  x: {
    label: "Date"
  },
  width: 800,  // Increased width for better visibility
  height: 500,  // Increased height for better visibility
  style: {
    fontFamily: "sans-serif",
    fontSize: 12
  },
  marginRight: 100  // Add right margin for the legend
})
```

```js
// Particle size spectra plot
const pss_plot = Plot.plot({
  marks: [
    Plot.dot(pss_filtered, {
      y: "mean_slope",
      x: "date",
      fill: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo),  // Use the custom color scale
      r: 3,
      opacity: 0.5,
      symbol: "park_depth"
    }),
    Plot.tip(pss_filtered, Plot.pointer({
      y: "mean_slope",
      x: "date",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nParking depth: ${d.park_depth} m`
    })),
    Plot.lineY(pss_filtered, Plot.windowY({
      k:12, 
      reduce: "median", 
      x: "date", 
      y: "mean_slope", 
      stroke: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo),
      strokeWidth: 3, 
      z: d => `${d.wmo}-${d.park_depth}`})),
    Plot.crosshair(pss_filtered, {x: "date", y: "mean_slope"})
  ],
  y: {
    label: "Mean slope",
    reverse: false
  },
  x: {
    label: "Date"
  },
  width: 800,  // Increased width for better visibility
  height: 500,  // Increased height for better visibility
  style: {
    fontFamily: "sans-serif",
    fontSize: 12
  },
  marginRight: 100  // Add right margin for the legend
})
```

```js
// Optical sediment trap plot
const ost_plot = Plot.plot({
  marks: [
    Plot.dot(ost_filtered, {
      y: "total_flux",
      x: "max_time",
      fill: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo),
      r: 3,
      opacity: 0.5,
      symbol: "park_depth"
    }),
    Plot.tip(ost_filtered, Plot.pointer({
      y: "total_flux",
      x: "max_time",
      title: d => `WMO: ${d.wmo}\nZone: ${d.zone}\nParking depth: ${d.park_depth} m\nSmall flux: ${d.small_flux.toFixed(2)}\nLarge flux: ${d.large_flux.toFixed(2)}`
    })),
    Plot.lineY(ost_filtered, Plot.windowY({
      k:12, 
      reduce: "median", 
      x: "max_time", 
      y: "total_flux", 
      stroke: d => colorByRegion ? colorScale(d.zone) : colorScale(d.wmo),
      strokeWidth: 3,
      z: d => `${d.wmo}-${d.park_depth}`})),
    Plot.crosshair(ost_filtered, {x: "max_time", y: "total_flux"})
  ],
  y: {
    label:  "Total particle flux (mg C m⁻² d⁻¹)",
    reverse: false
  },
  x: {
    label: "Date"
  },
  width: 800,  // Increased width for better visibility
  height: 500,  // Increased height for better visibility
  style: {
    fontFamily: "sans-serif",
    fontSize: 12
  },
  marginRight: 100  // Add right margin for the legend
})
```

```js
//const pointMax = Inputs.range([0, maxConcentration], {step: 1, value: maxConcentration, width: 60});
```

<div class="grid grid-cols-4" >
  <div class="card grid-colspan-2 grid-rowspan-1" style="padding: 0px;">
    <div style="padding: 1rem;">
      <h2><strong>Floats trajectories</strong></h2>
      <h3>Red markers show the last float position</h3>
      ${div}
    </div>
  </div>
  <div class="card grid-colspan-2 grid-rowspan-1">
    <h2><strong>Particle size spectra</strong></h2>
    <h3>A very negative slope in a particle size spectrum indicates that the concentration of particles decreases rapidly as the particle size increases.</h3>
    ${pss_plot}
</div>

<div class="card grid-colspan-2 grid-rowspan-1">
  <h2><strong>Particle concentrations at parking depth</strong></h2>
  <h3>Measured with the <a href="http://www.hydroptic.com/index.php/public/Page/product_item/UVP6-LP">Underwater Vision Profiler 6 (UVP6).</a></h3>
  <div style="display: flex; flex-direction: column; align-items: center;">
  </div>
  ${particle_plot}
</div>

<div class="card grid-colspan-2 grid-rowspan-0.5">
  <h2><strong>Total carbon flux derived from the optical sediment trap</strong></h2>
  <h3>Total (small and large) particle flux computed following the method described in <a href='https://doi.org/10.1029/2022GB007624'>Terrats et al., (2023)</a></h3>
  ${ost_plot}
</div>

<div class="small note">
  The Underwater Vision Profiler 6 (UVP6) is an underwater imaging system developed to measure the size and gray level of marine particles. A key feature of the UVP6 is its <a href= 'https://github.com/ecotaxa/uvpec'>integrated classification algorithm</a>, which can automatically categorize observed particles and organisms into various taxonomic groups.<br><br>
  The transmissometer measures the transmittance of a light beam at a given wavelength through a medium. In order to get the data presented above, the transmissometer, mounted on autonomous floats, is vertically oriented to measure the particle accumulation on the upward-facing optical window when the float is drifting (i.e., parked at a specific depth). As a result, the transmissometer operates as an optical sediment trap (OST).<br><br>
  A k-day moving median average has been applied to highlight the trends. k = 60 for the particle concentrations and k = 12 for the optical sediment trap and particle size spectra data.<br><br>
  Outliers for both the particle concentrations and optical sediment trap plots were removed using the <a href='https://en.wikipedia.org/wiki/Interquartile_range#Outliers'>IQR method</a>.<br><br>
  These data were collected and made freely available by the <a href="https://argo.ucsd.edu">International Argo Program</a> and the national programs that contribute to it. The Argo Program is part of the Global Ocean Observing System.
</div>