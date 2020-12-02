# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q3
# Purpose:      Shanghai projection
# programmer:   Zhe Liu
# Date:         2020-12-01
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Sample growth ----
## Shanghai sample
az.delivery <- read_xlsx('06_Deliveries/AZ_CHC_2017Q1_2020Q2_20201110_final.xlsx')

sh.sample <- az.delivery %>% 
  filter(City_C %in% c('上海'), 
         YQ %in% c('2019Q3'))

## Shanghai growth
sh.growth <- imp.total %>% 
  filter(city %in% c('北京'), 
         quarter %in% c('2019Q3', '2020Q3')) %>% 
  mutate(province = '上海', 
         city = '上海', 
         packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid)) %>% 
  group_by(quarter, city, packid) %>% 
  summarise(sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = quarter, 
              values_from = sales) %>% 
  mutate(growth = `2020Q3` / `2019Q3`, 
         growth = if_else(is.na(growth), 1, growth), 
         growth = if_else(growth > 3, 1, growth)) %>% 
  select(city, packid, growth)


##---- KNN model ----
## ims sales
raw.ims <- fread('02_Inputs/cn_IMS_Sales_Fdata_201912_1.txt', stringsAsFactors = FALSE)

ims.sales <- raw.ims %>% 
  mutate(date = gsub('M', '', Period_Code),
         packid = stri_pad_left(Pack_ID, 7, 0),
         sample_flag = if_else(packid %in% sh.growth$packid, 1, 0)) %>% 
  filter(Geography_id == 'CHT', date >= '201701', packid %in% sh.sample$Pack_ID) %>% 
  group_by(date, packid) %>% 
  summarise(sales = sum(LC, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(sample_flag = if_else(packid %in% sh.growth$packid, 1, 0)) %>% 
  pivot_wider(id_cols = c(packid, sample_flag), 
              names_from = date, 
              values_from = sales, 
              values_fill = 0)

## model set
train.sh <- ims.sales[ims.sales$sample_flag == 1, ]
test.sh <- ims.sales[ims.sales$sample_flag == 0, ]

## knn model
sh.model <- kknn(sample_flag ~ ., 
                 train = train.sh[, -1], 
                 test = test.sh[, -1], 
                 k = 3, 
                 scale = TRUE)

## model weight
sh.indice <- as.data.frame(sh.model$C) %>% 
  lapply(function(x) {
    train.sh$packid[x]
  }) %>% 
  as.data.frame(col.names = c('pack_1', 'pack_2', 'pack_3')) %>% 
  bind_cols(test.sh[, c('packid')]) %>% 
  pivot_longer(cols = -packid, 
               names_to = 'knn_level', 
               values_to = 'knn_pack')

sh.weight <- as.data.frame(sh.model$D) %>% 
  lapply(function(x) {
    1 / (x+1)
  }) %>% 
  as.data.frame(col.names = c('pack_1', 'pack_2', 'pack_3')) %>% 
  mutate(weight_sum = pack_1 + pack_2 + pack_3,
         pack_1 = pack_1 / weight_sum,
         pack_2 = pack_2 / weight_sum,
         pack_3 = pack_3 / weight_sum) %>% 
  bind_cols(test.sh[, c('packid')]) %>% 
  select(-weight_sum) %>% 
  pivot_longer(cols = -packid, 
               names_to = 'knn_level', 
               values_to = 'knn_weight')


##---- Growth ----
# growth
weight.growth <- sh.indice %>% 
  left_join(sh.weight, by = c('packid', 'knn_level')) %>% 
  left_join(sh.growth, by = c('knn_pack' = 'packid')) %>% 
  group_by(city, packid) %>% 
  summarise(growth = sum(growth * knn_weight, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(city, packid, growth)

# growth add
surplus <- setdiff(sh.sample$Pack_ID[!(sh.sample$Pack_ID %in% sh.growth$packid)], ims.sales$packid)

surplus.growth <- data.frame(city = '上海',
                             packid = surplus) %>% 
  mutate(growth = 1)

sh.growth.add <- bind_rows(merge(sh.growth, 0), 
                           merge(weight.growth, 1), 
                           merge(surplus.growth, 2))


##---- Price ----
sh.price.origin <- price.origin %>% 
  filter(city == '北京') %>% 
  mutate(province = '上海', 
         city = '上海')

sh.price.city <- price.city %>% 
  filter(city == '北京') %>% 
  mutate(province = '上海', 
         city = '上海')

sh.price.province <- price.province %>% 
  filter(province == '北京') %>% 
  mutate(province = '上海')

sh.price.year <- price.year %>% 
  filter(province == '北京') %>% 
  mutate(province = '上海')


##---- Result ----
## projection
proj.sh <- sh.sample %>% 
  left_join(sh.growth.add, by = c('City_C' = 'city', 'Pack_ID' = 'packid')) %>% 
  mutate(`Total Unit` = `Total Unit` * growth, 
         `Value (RMB)` = `Value (RMB)` * growth, 
         `Counting Unit` = `Counting Unit` * growth) %>% 
  filter(`Total Unit` > 0, `Value (RMB)` > 0) %>% 
  mutate(Year = '2020', 
         YQ = gsub('2019', '2020', YQ)) %>% 
  select(-growth, -y)

## other market
sh.cv <- proj.sh %>% 
  filter(Market %in% c('Crestor Market', 'Brilinta Market', 'HTN Market')) %>% 
  mutate(Market = 'CV Market')

sh.forxiga <- az.delivery %>% 
  filter(Market == 'Forxiga(SGLT2) Market', City_C == '上海') %>% 
  mutate(YQ = '2020Q3', 
         `Total Unit` = `Total Unit` * 1.333235, 
         `Value (RMB)` = `Value (RMB)` * 1.333235, 
         `Counting Unit` = `Counting Unit` * 1.333235)
## 北京安达唐环比

sh.ioad <- proj.sh %>% 
  filter(Market == 'Onglyza Market') %>% 
  bind_rows(sh.forxiga) %>% 
  mutate(Market = 'IOAD Market')

## result
az.chc.sh <- proj.sh %>% 
  filter(!(Market %in% c('CV Market', 'IOAD Market'))) %>% 
  bind_rows(sh.cv, sh.forxiga, sh.ioad) %>% 
  left_join(sh.price.origin, by = c('Province_C' = 'province', 'City_C' = 'city', 
                                    'YQ' = 'quarter', 'Pack_ID' = 'packid')) %>% 
  left_join(sh.price.city, by = c('Province_C' = 'province', 'City_C' = 'city', 
                                  'Year' = 'year', 'Pack_ID' = 'packid')) %>% 
  left_join(sh.price.province, by = c('Province_C' = 'province', 'YQ' = 'quarter', 
                                      'Pack_ID' = 'packid')) %>% 
  left_join(sh.price.year, by = c('Province_C' = 'province', 'Year' = 'year', 
                                  'Pack_ID' = 'packid')) %>% 
  left_join(price.pack, by = c('YQ' = 'quarter', 'Pack_ID' = 'packid')) %>% 
  left_join(price.pack.year, by = c('Year' = 'year', 'Pack_ID' = 'packid')) %>% 
  mutate(`价格` = price, 
         `价格` = if_else(is.na(`价格`), price_city, `价格`), 
         `价格` = if_else(is.na(`价格`), price_prov, `价格`), 
         `价格` = if_else(is.na(`价格`), price_year, `价格`), 
         `价格` = if_else(is.na(`价格`), price_pack, `价格`), 
         `价格` = if_else(is.na(`价格`), price_pack_year, `价格`), 
         `价格` = if_else(is.na(`价格`), `Value (RMB)` / `Total Unit`, `价格`), 
         `价格` = ifelse(Pack_ID == 'TCM Others', NA, `价格`)) %>% 
  mutate(`Total Unit` = if_else(Pack_ID == 'TCM Others', `Total Unit`, `Value (RMB)` / `价格`), 
         `Counting Unit` = ifelse(Pack_ID == 'TCM Others', NA, `Total Unit` * `转换比`)) %>% 
  select(Market, Year, YQ, Province_C, City_C, Province_E, City_E, Molecule_C, 
         PROD_DES_C, `剂型`, `规格`, `转换比`, `单位`, Pack_DESC, CORP_DES_C, 
         `购买方式`, `Total Unit`, `Value (RMB)`, `Counting Unit`, `价格`, 
         Pack_ID, `IMS 药品ID`, Mole_Ename, Prod_Ename, Corp_EName, Corp_TYPE, 
         `ATC Code IV`, TA, VBP_Excu, VBP, EDL_DESC)

write.xlsx(proj.sh, '03_Outputs/06_AZ_CHC_Projection_Shanghai.xlsx')


