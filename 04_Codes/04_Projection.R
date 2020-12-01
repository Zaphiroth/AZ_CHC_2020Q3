# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q3
# Purpose:      Projection
# programmer:   Zhe Liu
# Date:         2020-12-01
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Readin projection info ----
## city segment
segment <- read_xlsx('02_Inputs/seg_45cities.xlsx') %>% 
  mutate(seg_city = if_else(city == '上海', paste0(city, district), city)) %>% 
  select(seg_city, seg = seg_up)

## hospital universe
hospital.universe.raw <- pchc.universe %>% 
  group_by(pchc = PCHC_Code) %>% 
  summarise(province = first(na.omit(`省`)),
            city = first(na.omit(`地级市`)),
            district = first(na.omit(`区[县/县级市】`)),
            hospital = first(na.omit(`单位名称`)), 
            est = first(na.omit(`其中：西药药品收入（千元）`))) %>% 
  ungroup()

hospital.universe <- raw.total %>% 
  distinct(province, city, district, pchc) %>% 
  bind_rows(hospital.universe.raw) %>% 
  group_by(pchc) %>% 
  summarise(province = first(na.omit(province)),
            city = first(na.omit(city)),
            district = first(na.omit(district)), 
            est = first(na.omit(est))) %>% 
  ungroup() %>% 
  mutate(seg_city = if_else(city == '上海', paste0(city, district), city)) %>% 
  left_join(segment, by = 'seg_city') %>% 
  mutate(seg = if_else(is.na(seg), 1, seg))


##---- Projection ----
## projection data
proj.raw <- imp.total %>% 
  filter(quarter %in% c('2020Q3'))

## quarter sales
proj.quarter <- proj.raw %>% 
  group_by(quarter, province, city, pchc, TA, flag_mkt, packid) %>% 
  summarise(panel_sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup()

## universe set
universe.set <- merge(distinct(hospital.universe, 
                               province, city, district, pchc, seg, est), 
                      distinct(proj.raw, quarter)) %>% 
  left_join(distinct(proj.raw, 
                     province, city, district, TA, flag_mkt, atc4, molecule, 
                     product, packid), 
            by = c('province', 'city', 'district')) %>% 
  data.frame(stringsAsFactors = FALSE) %>% 
  mutate(seg = ifelse(is.na(seg), 1, seg),
         panel = ifelse(pchc %in% unique(raw.total$pchc), 1, 0),
         panel = ifelse(is.na(est), 0, panel))

filtered.set <- universe.set %>% 
  filter(panel == 1) %>% 
  left_join(proj.quarter, by = c('province', 'city', 'pchc', 'quarter', 
                                 'TA', 'flag_mkt', 'packid')) %>% 
  mutate(panel_sales = ifelse(is.na(panel_sales), 0, panel_sales))

## projection parameter
proj.parm <- data.table(filtered.set)[, {
  ux <- mean(est)
  uy <- mean(panel_sales)
  slope <- uy / ux
  intercept <- 0
  predict_sales <- slope * est
  spearman_cor <- cor(panel_sales, predict_sales, method = 'spearman')
  list(slope = slope, intercept = intercept, spearman_cor = spearman_cor)
}, by = list(city, quarter, packid, TA, flag_mkt, seg)]

## QC: TRUE means no multiple match. Continue. 
sum(filtered.set$panel_sales, na.rm = TRUE) <= sum(proj.quarter$panel_sales, na.rm = TRUE)


##---- Result ----
proj.total <- universe.set %>% 
  left_join(proj.quarter, by = c('province', 'city', 'pchc', 'quarter', 
                                 'TA', 'flag_mkt', 'packid')) %>% 
  mutate(panel_sales = ifelse(is.na(panel_sales), 0, panel_sales)) %>% 
  left_join(proj.parm, by = c('quarter', 'city', 'TA', 'flag_mkt', 'packid', 'seg')) %>% 
  mutate(predict_sales = est * slope + intercept, 
         final_sales = if_else(panel == 0, predict_sales, panel_sales), 
         final_sales = if_else(is.na(final_sales), panel_sales, final_sales)) %>% 
  filter(final_sales > 0) %>% 
  mutate(year = stri_sub(quarter, 1, 4)) %>% 
  group_by(year, quarter, province, city, district, pchc, TA, flag_mkt, atc4, 
           molecule, product, packid, panel) %>% 
  summarise(sales = sum(final_sales, na.rm = TRUE)) %>% 
  ungroup()

write.xlsx(proj.total, '03_Outputs/04_AZ_CHC_Projection.xlsx')


