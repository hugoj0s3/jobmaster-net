#Update swagger json everytime to generate new api and models
rm -rf openapi
mkdir openapi
openapi-generator-cli generate -g typescript-axios -i swagger.json --additional-properties=supportsES6=true,withSeparateModelsAndApi=true,modelPackage=models,apiPackage=apis -o ./openapi