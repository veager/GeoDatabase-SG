### Resale Flat Prices

- Download from: [Data.gov.sg](https://data.gov.sg/), [Resale Flat Prices](https://data.gov.sg/dataset/resale-flat-prices)
- create "address id": "block No." + "street name"
  - For example: 

### Acquire Location

- Use *Onemap* search API: [site](https://www.onemap.gov.sg/docs/#search)
  - pass *address id* on parameters `searchVal`

Acquire and save all HDB location:

```python
# acquire HDB address
zip_path = 'resale-flat-prices.zip'
hdb_price = read_hdb_price(zip_path)

save_path = 'hdb_address_{0}.csv'.format("2022-06")
req_hdb_addr_loc(hdb_addr=hdb_price, save_path=save_path)
```

