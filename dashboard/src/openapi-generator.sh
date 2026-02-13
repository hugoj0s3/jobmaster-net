#Update swagger json everytime to generate new api and models
rm -f ./lib/api/schema.d.ts
npx openapi-typescript swagger.json -o ./lib/api/schema.d.ts