# BI.DP.Airbyte
Custom Airbyte source connectors for marketing data extraction.

## Release flow example for release
dev2 -> dev -> qa -> stag --> prod
1. Push code to dev2 -> code will auto build the image -> manual update at airbyte
2. Push code to dev -> code will auto build the image -> manual update at airbyte
3. Push code to qa -> code will auto build the image -> manual update at airbyte
4. Push code to stag -> code will auto build the image -> manual update at airbyte
5. Push code to main -> code will auto build the image -> manual update at airbyte