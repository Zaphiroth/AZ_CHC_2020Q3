# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q2
# Purpose:      Summary
# programmer:   Zhe Liu
# Date:         2020-09-02
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Readin info ----
## pack info
product.info1 <- raw.servier.ahbjjs[, c(10:12, 14, 20, 31)] %>% 
  mutate(packcode = as.character(packcode)) %>% 
  bind_rows(raw.az.ah[, c(10:12, 14, 20, 31)], raw.az.bj[, c(10:12, 14, 20, 31)], 
            raw.az.fj[, c(10:12, 14, 20, 31)], raw.az.js[, c(10:12, 14, 20, 31)], 
            raw.az.sd[, c(10:12, 14, 20, 31)], raw.az.zj[, c(10:12, 14, 20, 31)], 
            raw.servier.sd[, c(10:12, 14, 20, 31)], 
            raw.pfizer.ah[, c(10:12, 14, 20, 31)], raw.pfizer.bj[, c(10:12, 14, 20, 31)], 
            raw.pfizer.js[, c(10:12, 14, 20, 31)], raw.pfizer.sd[, c(10:12, 14, 20, 31)]) %>% 
  filter(!is.na(packcode)) %>% 
  group_by(packid = packcode) %>% 
  summarise(Molecule_C = first(na.omit(Molecule_Desc_ZB)), 
            `剂型` = first(na.omit(Dosage)), 
            `规格` = first(na.omit(SPEC)), 
            `转换比` = first(na.omit(PACK)), 
            Pack_DESC = first(na.omit(Pck_Desc))) %>% 
  ungroup()

product.info2 <- read.xlsx('02_Inputs/Product standardization master data-A-S-1021.xlsx') %>% 
  distinct(packid = stri_pad_left(PACK_ID, 7, 0), Molecule_C = MOLE_NAME_CH, 
           PROD_DES_C = PROD_NAME_CH, Pack_DESC = PCK_DESC, `转换比` = PACK, 
           `剂型` = DOSAGE, `规格` = SPEC)

product.info3 <- read.xlsx('02_Inputs/packid_prod_20181112.xlsx') %>% 
  distinct(packid = stri_pad_left(pack_id, 7, 0), Molecule_C = gene_name, 
           PROD_DES_C = ims_product_cn, Pack_DESC = Pck_Desc, 
           `转换比` = PckSize_Desc) %>% 
  mutate(`剂型` = str_trim(str_replace(Pack_DESC, '[0-9]{1,}.{1,}', ''), 'right'),
         `规格` = sapply(Pack_DESC, function(x) {
           paste(str_extract_all(x, '[0-9]{1,}[a-zA-Z]{1,}', simplify = TRUE), collapse = ' ')
         }))

product.info4 <- ims.mol %>% 
  distinct(packid, 
           Pack_DESC = Pck_Desc) %>% 
  mutate(first_num_position = stri_locate_first(Pack_DESC, regex = '\\d')[,1],
         last_space_position = stri_locate_last(Pack_DESC, regex = '\\s')[,1],
         `剂型` = str_squish(substr(Pack_DESC, 1, first_num_position - 1)),
         `规格` = str_squish(substr(Pack_DESC, first_num_position, 
                                    last_space_position - 1)),
         `转换比` = as.integer(str_squish(substr(Pack_DESC, last_space_position, 
                                                 nchar(Pack_DESC)))))

product.info <- bind_rows(product.info2, product.info3, product.info4, product.info1) %>% 
  filter(!is.na(packid)) %>% 
  group_by(packid) %>% 
  summarise(Molecule_C = first(na.omit(Molecule_C)), 
            PROD_DES_C = first(na.omit(PROD_DES_C)), 
            `剂型` = first(na.omit(`剂型`)), 
            `规格` = first(na.omit(`规格`)), 
            `转换比` = first(na.omit(`转换比`)), 
            Pack_DESC = first(na.omit(Pack_DESC))) %>% 
  ungroup()

## corp info
corp.info <- read_xls("02_Inputs/Corp E & C name.xls") %>% 
  distinct(Corporation, Corporation_C) %>% 
  right_join(ims.mol, by = c('Corporation' = 'Corp_Desc')) %>% 
  distinct(packid, Corp_EName = Corporation, CORP_DES_C = Corporation_C, 
           Corp_TYPE = MNF_TYPE)

## city info
city.en <- read.xlsx('02_Inputs/City_CN_EN.xlsx')

## VBP info
vbp1 <- read.xlsx('02_Inputs/VBP匹配 for AZ CHC.xlsx', sheet = 2) %>% 
  mutate(city = gsub('市', '', `城市`),
         packid = stri_pad_left(Pack_Id, 7, 0)) %>% 
  distinct(city, packid, VBP_Excu1 = VBP_Excu, VBP1 = VBP)

vbp2 <- read.xlsx('02_Inputs/VBP匹配 for AZ CHC.xlsx', sheet = 3) %>% 
  mutate(province = gsub('省', '', `省份`),
         packid = stri_pad_left(Pack_Id, 7, 0)) %>% 
  distinct(province, packid, VBP_Excu2 = VBP_Excu, VBP2 = VBP)

vbp3 <- read.xlsx('02_Inputs/VBP匹配 for AZ CHC.xlsx', sheet = 4) %>% 
  mutate(packid = stri_pad_left(Pack_Id, 7, 0)) %>% 
  distinct(packid, VBP3 = VBP)

## EDL info
edl1 <- read.xlsx('02_Inputs/交付相关- EDL汇总.xlsx', sheet = 2) %>% 
  distinct(packid = PACK_COD, EDL_DESC1 = EDL_DESC)

edl2 <- read.xlsx('02_Inputs/交付相关- EDL汇总.xlsx', sheet = 3) %>% 
  distinct(molecule = Molecule.Composition, pack = stri_trim_both(Pack_m1), 
           EDL_DESC2 = EDL_DESC) %>% 
  filter(!(molecule == 'TRADITIONAL CHINESE MEDICINE' & pack == 'CAP 300MG')) %>% 
  filter(!(molecule == 'EPOETIN(UNSPECIFIED)' & pack == 'AMP 3000IU 1ML'))


##---- Result ----
az.delivery <- read_xlsx('06_Deliveries/AZ_CHC_2017Q1_2020Q2_20201110_final.xlsx')

## CV Market
az.chc.cv <- proj.price %>% 
  mutate(packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid), 
         atc4 = if_else(packid == 'TCM Others', 'TCM Others', atc4), 
         molecule = if_else(packid == 'TCM Others', 'TCM Others', molecule), 
         product = if_else(packid == 'TCM Others', 'TCM Others', product)) %>% 
  group_by(year, quarter, province, city, TA, flag_mkt, atc4, molecule, product, packid) %>% 
  summarise(units = sum(units, na.rm = TRUE), 
            sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(market.mapping, by = 'flag_mkt') %>% 
  left_join(product.info, by = 'packid') %>% 
  filter(`小市场` %in% c('Crestor Market', 'Brilinta Market', 'HTN Market')) %>% 
  mutate(`小市场` = 'CV Market', 
         `购买方式` = 'ATC+商品名+类别')

## PPI IV Market & PPI Oral Market
az.chc.ppi <- proj.price %>% 
  mutate(packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid), 
         atc4 = if_else(packid == 'TCM Others', 'TCM Others', atc4), 
         molecule = if_else(packid == 'TCM Others', 'TCM Others', molecule), 
         product = if_else(packid == 'TCM Others', 'TCM Others', product)) %>% 
  group_by(year, quarter, province, city, TA, flag_mkt, atc4, molecule, product, packid) %>% 
  summarise(units = sum(units, na.rm = TRUE), 
            sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(market.mapping, by = 'flag_mkt') %>% 
  left_join(product.info, by = 'packid') %>% 
  filter(`小市场` %in% c('PPI (Oral/IV) Market')) %>% 
  mutate(`小市场` = if_else(`剂型` %in% c('粉针剂'), 
                         'PPI IV Market', 'PPI Oral Market'))

## XZK Market(Excl Potent Statin)
az.chc.xzk <- proj.price %>% 
  left_join(market.mapping, by = 'flag_mkt') %>% 
  left_join(product.info, by = 'packid') %>% 
  filter(`小市场` %in% c('XZK Market')) %>% 
  filter(!(molecule %in% c('ATORVASTATIN+AMLODIPINE', 'ATORVASTATIN', 'ROSUVASTATIN'))) %>% 
  mutate(`小市场` = 'XZK Market(Excl Potent Statin)') %>% 
  mutate(packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid), 
         atc4 = if_else(packid == 'TCM Others', 'TCM Others', atc4), 
         molecule = if_else(packid == 'TCM Others', 'TCM Others', molecule), 
         product = if_else(packid == 'TCM Others', 'TCM Others', product)) %>% 
  group_by(year, quarter, province, city, TA, flag_mkt, atc4, molecule, product, 
           packid, `小市场`, `大市场`, `购买方式`, Molecule_C, PROD_DES_C, 
           `剂型`, `规格`, `转换比`, Pack_DESC) %>% 
  summarise(units = sum(units, na.rm = TRUE), 
            sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup()

## IOAD Market
az.chc.ioad <- proj.price %>% 
  mutate(packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid), 
         atc4 = if_else(packid == 'TCM Others', 'TCM Others', atc4), 
         molecule = if_else(packid == 'TCM Others', 'TCM Others', molecule), 
         product = if_else(packid == 'TCM Others', 'TCM Others', product)) %>% 
  group_by(year, quarter, province, city, TA, flag_mkt, atc4, molecule, product, packid) %>% 
  summarise(units = sum(units, na.rm = TRUE), 
            sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(market.mapping, by = 'flag_mkt') %>% 
  left_join(product.info, by = 'packid') %>% 
  filter(`小市场` %in% c('Onglyza Market', 'Forxiga(SGLT2) Market')) %>% 
  mutate(`小市场` = 'IOAD Market', 
         `购买方式` = '商品名')

## CHC
az.chc <- proj.price %>% 
  mutate(packid = if_else(stri_sub(packid, 1, 5) %in% market.cndrug$PROD_COD, 
                          'TCM Others', packid), 
         atc4 = if_else(packid == 'TCM Others', 'TCM Others', atc4), 
         molecule = if_else(packid == 'TCM Others', 'TCM Others', molecule), 
         product = if_else(packid == 'TCM Others', 'TCM Others', product)) %>% 
  group_by(year, quarter, province, city, TA, flag_mkt, atc4, molecule, product, packid) %>% 
  summarise(units = sum(units, na.rm = TRUE), 
            sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  left_join(market.mapping, by = 'flag_mkt') %>% 
  left_join(product.info, by = 'packid') %>% 
  bind_rows(az.chc.cv, az.chc.ppi, az.chc.xzk, az.chc.ioad) %>% 
  left_join(city.en, by = c('province' = 'Province_C', 'city' = 'City_C')) %>% 
  left_join(corp.info, by = 'packid') %>% 
  left_join(vbp1, by = c('city', 'packid')) %>% 
  left_join(vbp2, by = c('province', 'packid')) %>% 
  left_join(vbp3, by = c('packid')) %>% 
  left_join(edl1, by = 'packid') %>% 
  mutate(pack = stri_trim_both(stri_sub(Pack_DESC, 1, -4)), 
         Pack_DESC = stri_paste(stri_trim_right(gsub('.{3}$', '', product)), 
                                ' ', Pack_DESC), 
         Pack_DESC = stri_paste(stri_trim_right(gsub('.{3}$', '', Pack_DESC)), 
                                ' ', stri_trim_both(stri_sub(Pack_DESC, -3, -1)))) %>% 
  left_join(edl2, by = c('molecule', 'pack')) %>% 
  mutate(Molecule_C = if_else(packid == 'TCM Others', 'TCM Others', Molecule_C), 
         PROD_DES_C = if_else(packid == 'TCM Others', 'TCM Others', PROD_DES_C), 
         Pack_DESC = if_else(packid == 'TCM Others', 'TCM Others', Pack_DESC), 
         CORP_DES_C = if_else(packid == 'TCM Others', 'TCM Others', CORP_DES_C), 
         Corp_EName = if_else(packid == 'TCM Others', 'TCM Others', Corp_EName), 
         `IMS 药品ID` = stri_paste(stri_sub(packid, 1, 5), '-', packid), 
         `IMS 药品ID` = if_else(packid == 'TCM Others', 'TCM Others', `IMS 药品ID`), 
         VBP_Excu = ifelse(is.na(VBP_Excu1), VBP_Excu2, VBP_Excu1), 
         VBP = ifelse(is.na(VBP1), VBP2, VBP1), 
         VBP = ifelse(is.na(VBP), VBP3, VBP), 
         EDL_DESC = if_else(is.na(EDL_DESC1), EDL_DESC2, EDL_DESC1), 
         `Total Unit` = units, 
         `Value (RMB)` = sales, 
         `Counting Unit` = units * `转换比`, 
         `价格` = sales / units,
         `单位` = NA_character_) %>% 
  filter(!(`小市场` == 'Non-Oral Expectorant Market' & 
             !(`剂型` %in% c('粉针剂', '冻干粉针剂', '雾化溶液', '吸入剂', 
                           '吸入溶液剂', '注射液', '小容量注射液', '大容量注射液')))) %>% 
  select(Market = `小市场`, 
         Year = year, 
         YQ = quarter, 
         Province_C = province, 
         City_C = city, 
         Province_E, 
         City_E, 
         Molecule_C, 
         PROD_DES_C, 
         `剂型`, 
         `规格`, 
         `转换比`, 
         `单位`, 
         Pack_DESC, 
         CORP_DES_C, 
         `购买方式`, 
         `Total Unit`, 
         `Value (RMB)`, 
         `Counting Unit`, 
         `价格`, 
         Pack_ID = packid, 
         `IMS 药品ID`, 
         Mole_Ename = molecule, 
         Prod_Ename = product, 
         Corp_EName, 
         Corp_TYPE, 
         `ATC Code IV` = atc4, 
         TA, 
         VBP_Excu, 
         VBP, 
         EDL_DESC)

## total
az.chc.total <- bind_rows(az.delivery, az.chc, az.chc.sh) %>% 
  group_by(Pack_ID) %>% 
  mutate(Pack_DESC = first(na.omit(Pack_DESC))) %>% 
  ungroup() %>% 
  mutate(`剂型` = if_else(Pack_ID == 'TCM Others', NA_character_, `剂型`), 
         `规格` = if_else(Pack_ID == 'TCM Others', NA_character_, `规格`), 
         `转换比` = ifelse(Pack_ID == 'TCM Others', NA, `转换比`), 
         `ATC Code IV` = if_else(Pack_ID == 'TCM Others', NA_character_, `ATC Code IV`), 
         CORP_DES_C = case_when(
           Corp_EName == 'HLJ.TIANNIAN PHARM' ~ '天年药业(哈尔滨)有限公司', 
           Corp_EName == 'FJ.DAWNRAYS PHARMA' ~ '福建东瑞制药有限公司', 
           TRUE ~ CORP_DES_C
         ), 
         Corp_TYPE = case_when(
           Corp_EName == 'GD.TONGYUAN PHARM' ~ 'L', 
           Corp_EName == 'SX.HUAYUAN PHARM' ~ 'L', 
           Corp_EName == 'MERCK GROUP' ~ 'I', 
           Corp_EName == 'ZAMBON GROUP' ~ 'I', 
           Corp_EName == 'GD.SZH JIURUI HEAL' ~ 'L', 
           Corp_EName == 'NJ.HICIN PHARM' ~ 'L', 
           Corp_EName == 'SD.FENGHUANG PHARM' ~ 'L', 
           Corp_EName == 'FJ.DAWNRAYS PHARMA' ~ 'L', 
           TRUE ~ Corp_TYPE
         ), 
         VBP = if_else(Pack_ID == 'TCM Others', 'N', VBP)) %>% 
  group_by(Market, TA, Pack_ID) %>% 
  mutate(Molecule_C = first(na.omit(Molecule_C)), 
         PROD_DES_C = first(na.omit(PROD_DES_C)), 
         `剂型` = first(na.omit(`剂型`)), 
         `规格` = first(na.omit(`规格`)), 
         `转换比` = first(na.omit(`转换比`)), 
         `单位` = first(na.omit(`单位`)), 
         Pack_DESC = first(na.omit(Pack_DESC)), 
         CORP_DES_C = first(na.omit(CORP_DES_C)), 
         `购买方式` = first(na.omit(`购买方式`)), 
         `IMS 药品ID` = first(na.omit(`IMS 药品ID`)), 
         Mole_Ename = first(na.omit(Mole_Ename)), 
         Prod_Ename = first(na.omit(Prod_Ename)), 
         Corp_EName = first(na.omit(Corp_EName)), 
         Corp_TYPE = first(na.omit(Corp_TYPE)), 
         `ATC Code IV` = first(na.omit(`ATC Code IV`)), 
         VBP_Excu = first(na.omit(VBP_Excu)), 
         VBP = first(na.omit(VBP)), 
         EDL_DESC = first(na.omit(EDL_DESC))) %>% 
  ungroup() %>% 
  group_by(Market, Year, YQ, Province_C, City_C, Province_E, City_E, Molecule_C, 
           PROD_DES_C, `剂型`, `规格`, `转换比`, `单位`, Pack_DESC, CORP_DES_C, 
           `购买方式`, Pack_ID, `IMS 药品ID`, Mole_Ename, Prod_Ename, Corp_EName, 
           Corp_TYPE, `ATC Code IV`, TA, VBP_Excu, VBP, EDL_DESC) %>% 
  summarise(`Total Unit` = sum(`Total Unit`, na.rm = TRUE), 
            `Value (RMB)` = sum(`Value (RMB)`, na.rm = TRUE), 
            `Counting Unit` = sum(`Counting Unit`, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(`价格` = `Value (RMB)` / `Total Unit`, 
         `价格` = ifelse(Pack_ID == 'TCM Others', NA, `价格`), 
         `Counting Unit` = ifelse(Pack_ID == 'TCM Others', NA, `Counting Unit`)) %>% 
  filter(`Total Unit` > 0, `Value (RMB)` > 0) %>% 
  select(Market, Year, YQ, Province_C, City_C, Province_E, City_E, Molecule_C, 
         PROD_DES_C, `剂型`, `规格`, `转换比`, `单位`, Pack_DESC, CORP_DES_C, 
         `购买方式`, `Total Unit`, `Value (RMB)`, `Counting Unit`, `价格`, 
         Pack_ID, `IMS 药品ID`, Mole_Ename, Prod_Ename, Corp_EName, Corp_TYPE, 
         `ATC Code IV`, TA, VBP_Excu, VBP, EDL_DESC) %>% 
  arrange(YQ, City_C, Market, Pack_ID)

write.xlsx(az.chc.total, '03_Outputs/07_AZ_CHC_2017Q1_2020Q3.xlsx')

## check Pack ID
chk <- az.chc.total %>% 
  add_count(Market, YQ, City_C, Pack_ID) %>% 
  filter(n > 1)

## check pack info
chk <- az.chc.total %>% 
  distinct(Market, Pack_ID, Molecule_C, PROD_DES_C, `剂型`, `规格`, `转换比`, 
           Pack_DESC, CORP_DES_C, `IMS 药品ID`, Mole_Ename, Prod_Ename, Corp_EName, 
           Corp_TYPE, `ATC Code IV`, TA, VBP_Excu, VBP, EDL_DESC) %>% 
  add_count(Market, TA, Pack_ID) %>% 
  filter(n > 1)
