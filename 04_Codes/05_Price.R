# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q3
# Purpose:      Price
# programmer:   Zhe Liu
# Date:         2020-12-02
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Origin Price ----
price.origin <- raw.total %>% 
  group_by(packid, quarter, province, city) %>% 
  summarise(sales = sum(sales, na.rm = TRUE),
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price = sales / units) %>% 
  select(-sales, -units)


##---- Mean price by city year ----
price.city <- raw.total %>% 
  group_by(packid, year, province, city) %>% 
  summarise(sales = sum(sales, na.rm = TRUE), 
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price_city = sales / units) %>% 
  select(-sales, -units)


##---- Mean price by province quarter ----
price.province <- raw.total %>% 
  group_by(packid, quarter, province) %>% 
  summarise(sales = sum(sales, na.rm = TRUE),
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price_prov = sales / units) %>% 
  select(-sales, -units)


##---- Mean price by province year ----
price.year <- raw.total %>% 
  group_by(packid, year, province) %>% 
  summarise(sales = sum(sales, na.rm = TRUE), 
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price_year = sales / units) %>% 
  select(-sales, -units)


##---- Mean price by pack quarter ----
price.pack <- raw.total %>% 
  group_by(packid, quarter) %>% 
  summarise(sales = sum(sales, na.rm = TRUE),
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price_pack = sales / units) %>% 
  select(-sales, -units)


##---- Mean price by pack year ----
price.pack.year <- raw.total %>% 
  group_by(packid, year) %>% 
  summarise(sales = sum(sales, na.rm = TRUE),
            units = sum(units, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(price_pack_year = sales / units) %>% 
  select(-sales, -units)


##---- Add new price ----
proj.price <- proj.total %>% 
  left_join(price.origin, by = c('province', 'city', 'quarter', 'packid')) %>% 
  left_join(price.city, by = c('province', 'city', 'year', 'packid')) %>% 
  left_join(price.province, by = c('province', 'quarter', 'packid')) %>% 
  left_join(price.year, by = c('province', 'year', 'packid')) %>% 
  left_join(price.pack, by = c('quarter', 'packid')) %>% 
  left_join(price.pack.year, by = c('year', 'packid')) %>% 
  mutate(price = if_else(is.na(price), price_city, price), 
         price = if_else(is.na(price), price_prov, price), 
         price = if_else(is.na(price), price_year, price), 
         price = if_else(is.na(price), price_pack, price), 
         price = if_else(is.na(price), price_pack_year, price)) %>% 
  mutate(units = sales / price) %>% 
  filter(units > 0, sales > 0, price > 0) %>% 
  select(year, quarter, province, city, district, pchc, TA, flag_mkt, atc4, 
         molecule, product, packid, units, sales, price, panel)

write.xlsx(proj.price, '03_Outputs/05_AZ_CHC_Projection_with_Price.xlsx')

