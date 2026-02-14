#Update swagger json everytime to generate new api and models
rm -f ./lib/api/schema.d.ts
node -e "const fs=require('fs'); const spec=JSON.parse(fs.readFileSync('swagger.json','utf8')); const out={...spec, paths:{}}; for(const [p,v] of Object.entries(spec.paths||{})){ const np=p.replace(/^\/jm-api(\/|$)/,'/'); out.paths[np]=v; } fs.writeFileSync('swagger.stripped.json', JSON.stringify(out, null, 2));"
npx openapi-typescript swagger.stripped.json -o ./lib/api/schema.d.ts
rm -f swagger.stripped.json