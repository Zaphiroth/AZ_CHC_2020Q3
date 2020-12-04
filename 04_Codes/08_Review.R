# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  Servier CHC 2020Q3
# Purpose:      Review
# programmer:   Zhe Liu
# date:         2020-11-20
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Check SOP -----
## CHPA
chpa.format <- read.xlsx('05_Internal_Review/ims_chpa_to20Q3_format.xlsx')

market.pack <- az.chc.total %>% 
  distinct(Pack_ID, Market)

az.chpa <- chpa.format %>% 
  pivot_longer(cols = c(ends_with('UNIT'), ends_with('RENMINBI')), 
               names_to = 'quarter', 
               values_to = 'value') %>% 
  separate(quarter, c('quarter', 'measure'), sep = '_') %>% 
  pivot_wider(id_cols = c(Pack_ID, ATC4_Code, Molecule_Desc, Prd_desc, 
                          Pck_Desc, Corp_Desc, quarter), 
              names_from = measure, 
              values_from = value) %>% 
  left_join(market.pack, by = 'Pack_ID') %>% 
  filter(!is.na(Market), 
         stri_sub(quarter, 1, 4) %in% c('2018', '2019', '2020')) %>% 
  select(Pack_ID, YQ = quarter, `ATC Code IV` = ATC4_Code, Market, 
         Mole_Ename = Molecule_Desc, Prod_Ename = Prd_desc, Pack_DESC = Pck_Desc, 
         Corp_EName = Corp_Desc, `Total Unit` = UNIT, `Value (RMB)` = RENMINBI)

write.xlsx(az.chpa, '05_Internal_Review/AZ_CHPA_2018Q1_2020Q3.xlsx')

## Update
chpa.info <- az.chpa %>% 
  distinct(Pack_ID, `ATC Code IV`, Mole_Ename, Prod_Ename, Pack_DESC, Corp_EName)

delivery.update <- az.chc.total %>% 
  distinct(Pack_ID, Province_C, City_C, YQ, Market, 
           `Value (RMB)`, `Total Unit`, `Counting Unit`) %>% 
  left_join(chpa.info, by = 'Pack_ID') %>% 
  filter(!is.na(`ATC Code IV`), stri_sub(YQ, 1, 4) %in% c('2018', '2019', '2020'))

write.xlsx(delivery.update, '05_Internal_Review/AZ_Delivery_Updated.xlsx')

## internal
az.internal <- read.xlsx('05_Internal_Review/2017_2020_AZ_Internal Sales.xlsx')
internal.pack.mapping <- read.xlsx('05_Internal_Review/internal_packid_mapping.xlsx')

az.internal.city <- az.internal %>% 
  mutate(CityNameC = gsub('市', '', CityNameC)) %>% 
  group_by(YQtr, ProvinceNameC, CityNameC, BrandNameE, SkuNameE) %>% 
  summarise(`Value (RMB)` = sum(Actual, na.rm = TRUE), 
            `Total Unit` = sum(ActualQty, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(internal.pack.mapping, by = c('SkuNameE' = 'SKU_E_NAME')) %>% 
  filter(!is.na(Packid)) %>% 
  mutate(Pack_ID = stri_pad_left(Packid, 7, 0)) %>% 
  left_join(market.pack, by = 'Pack_ID') %>% 
  filter(!is.na(Market), stri_sub(YQtr, 1, 4) %in% c('2018', '2019', '2020')) %>% 
  left_join(chpa.info, by = 'Pack_ID') %>% 
  select(Market, YQ = YQtr, ProvinceNameC, CityNameC, Pack_ID, `ATC Code IV`, 
         Mole_Ename, Prod_Ename, Pack_DESC, Corp_EName, `Total Unit`, `Value (RMB)`)

write.xlsx(az.internal.city, '05_Internal_Review/AZ_Internal_City.xlsx')

## price
chpa.cu <- read.xlsx('05_Internal_Review/CHPA_Product_Selection.xlsx')

prod.sel <- chpa.cu %>% 
  left_join(market.pack, by = 'Pack_ID') %>% 
  filter(!is.na(Market)) %>% 
  group_by(Market) %>% 
  arrange(-CU_2020Q3) %>% 
  mutate(cumprop = cumsum(CU_2020Q3) / sum(CU_2020Q3)) %>% 
  filter(cumprop <= 0.5) %>% 
  ungroup() %>% 
  select(Market, Pack_ID) %>% 
  mutate(flag = 1)

az.price <- az.chc.total %>% 
  filter(Pack_ID != 'TCM Others') %>% 
  group_by(Year, YQ, Province_C, City_C, Market, Prod_Ename, Pack_ID) %>% 
  summarise(`Value (RMB)` = sum(`Value (RMB)`, na.rm = TRUE), 
            `Total Unit` = sum(`Total Unit`, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(prod.sel, by = c('Market', 'Pack_ID')) %>% 
  filter(flag == 1) %>% 
  mutate(price = `Value (RMB)` / `Total Unit`)

write.xlsx(az.price, '05_Internal_Review/AZ_CHC_Price.xlsx')


