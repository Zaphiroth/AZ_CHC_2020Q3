# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q3
# Purpose:      Summary table
# programmer:   Zhe Liu
# Date:         2020-12-14
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- Delivery data ----
# data
az.delivery.final <- read_xlsx('06_Deliveries/AZ_CHC_2017Q1_2020Q3_20201211_final.xlsx')

table.market <- c('CV Market', 'NIAD Market', 'PPI (Oral/IV) Market', 
                  'Linaclotide Market', 'Respules (Asthma&COPD) Market')

table.prod <- c('SYMBICORT TURBUHAL AZN', 'PULMICORT RESP     AZN', 
                'BRICANYL           AZM', 'FLUIMUCIL          ZAM', 
                'NEXIUM             AZN', 'LOSEC              AZN', 
                'LOSEC              AZM', 'LINZESS            AZN', 
                'CRESTOR            AZN', 'XUE ZHI KANG       BWX', 
                'BETALOC ZOK        AZN', 'BRILINTA           AZN', 
                'PLENDIL            AZM', 'ONGLYZA            AZN', 
                'FORXIGA            AZN', 'KOMBIGLYZE XR      AZN')

table.year <- c('2018', '2019', '2020')

table.quarter <- '2020Q2'

table.data <- az.delivery.final %>% 
  group_by(Year, YQ, City_E, TA, Market, Prod_Ename, Corp_EName, Corp_TYPE) %>% 
  summarise(value = sum(`Value (RMB)` / 7.2, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(TA %in% c('RE', 'CV', 'DM', 'GI'), 
         Market %in% table.market)

ytd.quarter <- table.data %>% 
  filter(Year == '2020') %>% 
  mutate(quarter = stri_sub(YQ, 5, 6)) %>% 
  distinct(quarter) %>% 
  unlist()


##---- S3-1 ----
s31.total <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year = stri_paste(Year, max(ytd.quarter), ' YTD'), Growth = 'Total') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1)

s31.mnc <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  filter(Corp_TYPE %in% c('I', 'J')) %>% 
  group_by(Year = stri_paste(Year, max(ytd.quarter), ' YTD'), Growth = 'MNC') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1)

s31.local <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  filter(Corp_TYPE %in% c('L')) %>% 
  group_by(Year = stri_paste(Year, max(ytd.quarter), ' YTD'), Growth = 'Local') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1)

s31.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(Year = stri_paste(Year, max(ytd.quarter), ' YTD'), Growth = 'AZ') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1)

s31 <- bind_rows(s31.total, s31.mnc, s31.local, s31.az) %>% 
  filter(stri_sub(Year, 1, 4) == stri_sub(table.quarter, 1, 4)) %>% 
  pivot_wider(id_cols = Growth, 
              names_from = Year, 
              values_from = growth)


##---- S3-2 ----
s32.total <- table.data %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, Growth = 'Total') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter) %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s32.mnc <- table.data %>% 
  filter(Corp_TYPE %in% c('I', 'J')) %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, Growth = 'MNC') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter) %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s32.local <- table.data %>% 
  filter(Corp_TYPE %in% c('L')) %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, Growth = 'Local') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter) %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s32.az <- table.data %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, Growth = 'AZ') %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter) %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s32 <- bind_rows(s32.total, s32.mnc, s32.local, s32.az) %>% 
  filter(Year >= '2018') %>% 
  pivot_wider(id_cols = Growth, 
              names_from = YQ, 
              values_from = growth)


##---- S3-3 ----
s33.market <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(TA) %>% 
  arrange(Year) %>% 
  mutate(`MKT GR%` = value / lag(value) - 1) %>% 
  ungroup()

s33.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(TA, Corp_EName) %>% 
  arrange(Year) %>% 
  mutate(`AZ GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  group_by(Year, TA) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE), 
         `AZ Rank` = rank(-value, ties.method = 'first')) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(TA) %>% 
  arrange(Year) %>% 
  mutate(`AZ ΔMS%` = `AZ MS%` - lag(`AZ MS%`)) %>% 
  ungroup()

s33 <- left_join(s33.market, s33.az, by = c('TA', 'Year')) %>% 
  filter(Year == stri_sub(table.quarter, 1, 4), 
         TA %in% c('CV', 'DM', 'RE', 'GI')) %>% 
  mutate(TA = factor(TA, levels = c('CV', 'DM', 'RE', 'GI'))) %>% 
  arrange(TA) %>% 
  select(TA, `MKT GR%`, `AZ GR%`, `AZ MS%`, `AZ ΔMS%`, `AZ Rank`) %>% 
  pivot_longer(cols = c(`MKT GR%`, `AZ GR%`, `AZ MS%`, `AZ ΔMS%`, `AZ Rank`), 
               names_to = 'Index', 
               values_to = 'value') %>% 
  pivot_wider(names_from = TA, 
              values_from = value)


##---- S4-1 ----
s41.ta <- table.data %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  group_by(Year, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E) %>% 
  mutate(city_value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  arrange(-city_value) %>% 
  pivot_wider(id_cols = City_E, 
              names_from = TA, 
              values_from = value)

s41.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Year, City_E) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(City_E) %>% 
  arrange(Year) %>% 
  mutate(`AZ EI` = `AZ MS%` / lag(`AZ MS%`)) %>% 
  ungroup() %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  select(City_E, `AZ EI`, `AZ MS%`)

s41 <- left_join(s41.ta, s41.az, by = 'City_E')


##---- S4-2 ----
s42.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Year, City_E) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE), 
         `AZ Rank` = rank(-value, ties.method = 'first')) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(City_E) %>% 
  arrange(Year) %>% 
  mutate(`AZ ΔMS%` = `AZ MS%` - lag(`AZ MS%`)) %>% 
  ungroup() %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  select(City_E, `AZ Rank`, `AZ ΔMS%`)

s42.competitor <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Corp_EName != 'ASTRAZENECA GROUP') %>% 
  group_by(Year, City_E) %>% 
  filter(value == max(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  select(City_E, `Top Competitor` = Corp_EName)

s42 <- left_join(s42.az, s42.competitor, by = 'City_E') %>% 
  mutate(`AZ Rank` = as.character(`AZ Rank`), 
         `AZ ΔMS%` = stri_paste(round(`AZ ΔMS%` * 100, 2), '%')) %>% 
  pivot_longer(cols = c(`AZ Rank`, `AZ ΔMS%`, `Top Competitor`),
               names_to = 'Index',
               values_to = 'Value') %>% 
  mutate(City_E = factor(City_E, levels = unique(s41$City_E))) %>% 
  arrange(City_E) %>% 
  pivot_wider(names_from = City_E,
              values_from = Value)


##---- S5-8 ----
s58.total <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E = 'Total', TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup()

s58.market <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  bind_rows(s58.total) %>% 
  group_by(Year, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA) %>% 
  arrange(Year) %>% 
  mutate(`MKT GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  select(-value)

s58.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  bind_rows(s58.total) %>% 
  group_by(Year, City_E, TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Corp_EName) %>% 
  arrange(Year) %>% 
  mutate(`AZ GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  group_by(Year, City_E, TA) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(City_E, TA) %>% 
  arrange(Year) %>% 
  mutate(`AZ ΔMS%` = `AZ MS%` - lag(`AZ MS%`)) %>% 
  ungroup()

s58 <- left_join(s58.az, s58.market, by = c('TA', 'Year', 'City_E')) %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  arrange(-value) %>% 
  select(TA, City_E, `AZ MS%`, `AZ ΔMS%`, `AZ Sales` = value, `AZ GR%`, `MKT GR%`)


##---- S10-1 ----
s101 <- table.data %>% 
  filter(Year >= '2018') %>% 
  group_by(YQ, City_E, TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(YQ, City_E, TA) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  mutate(Measure = 'AZ MS%') %>% 
  arrange(TA, YQ) %>% 
  pivot_wider(id_cols = c(City_E, TA, Measure), 
              names_from = YQ, 
              values_from = `AZ MS%`)


##---- S10-2 ----
s102.az <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Corp_EName) %>% 
  arrange(Year) %>% 
  mutate(`AZ GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  group_by(Year, City_E, TA) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE), 
         `AZ Rank` = rank(-value, ties.method = 'first')) %>% 
  ungroup() %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(City_E, TA) %>% 
  arrange(Year) %>% 
  mutate(`AZ ΔMS%` = `AZ MS%` - lag(`AZ MS%`)) %>% 
  ungroup()

s102.competitor <- table.data %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Corp_EName) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by( Year, City_E, TA) %>% 
  mutate(`Competitor MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(Corp_EName != 'ASTRAZENECA GROUP') %>% 
  group_by(Year, City_E, TA) %>% 
  filter(value == max(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(TA, Year, City_E, `Top competitor` = Corp_EName, `Competitor MS%`)

s102 <- left_join(s102.az, s102.competitor, by = c('TA', 'Year', 'City_E')) %>% 
  filter(Year == stri_sub(table.quarter, 1, 4), 
         TA %in% c('RE', 'CV', 'DM', 'GI')) %>% 
  mutate(`MS% Gap vs Top Player` = `AZ MS%` - `Competitor MS%`) %>% 
  select(City_E, TA, `AZ Rank`, `AZ GR%`, `AZ MS%`, `AZ ΔMS%`, 
         `Top competitor`, `MS% Gap vs Top Player`)


##---- S11-1 ----
s111 <- az.delivery.final %>% 
  group_by(Year, YQ, City_E, TA, Market, Prod_Ename, Corp_EName, Corp_TYPE) %>% 
  summarise(value = sum(`Value (RMB)` / 7.2, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Market) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market) %>% 
  arrange(Year) %>% 
  mutate(`Market GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  select(City_E, TA, Market, `Market GR%`)


##---- S11-2 ----
s112.order <- read_excel('02_Inputs/S11-2 Mapping.xlsx')

s112 <- az.delivery.final %>% 
  group_by(Year, YQ, City_E, TA, Market, Prod_Ename, Corp_EName, Corp_TYPE) %>% 
  summarise(value = sum(`Value (RMB)` / 7.2, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Market, Corp_EName, Prod_Ename) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market, Corp_EName, Prod_Ename) %>% 
  arrange(Year) %>% 
  mutate(`YOY GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market) %>% 
  mutate(`YOY MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market, Corp_EName, Prod_Ename) %>% 
  arrange(Year) %>% 
  mutate(`ΔMS%` = `YOY MS%` - lag(`YOY MS%`)) %>% 
  ungroup() %>% 
  filter(Year == stri_sub(table.quarter, 1, 4)) %>% 
  left_join(s112.order, by = 'Prod_Ename') %>% 
  select(City_E, TA, Market, Corp_EName, Prod_Ename, `Prod_Ename PPT`, `YOY GR%`, `ΔMS%`)


##---- S12-1 ----
s121.total <- table.data %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter, City_E, TA, Growth = 'Total') %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s121.mnc <- table.data %>% 
  filter(Corp_TYPE %in% c('I', 'J')) %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter, City_E, TA, Growth = 'MNC') %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s121.local <- table.data %>% 
  filter(!(Corp_TYPE %in% c('I', 'J'))) %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter, City_E, TA, Growth = 'Local') %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s121.az <- table.data %>% 
  filter(Corp_EName == 'ASTRAZENECA GROUP') %>% 
  group_by(Year, Quarter = stri_sub(YQ, 5, 6), YQ, City_E, TA) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Quarter, City_E, TA, Growth = 'AZ') %>% 
  arrange(Year) %>% 
  mutate(growth = value / lag(value) - 1) %>% 
  ungroup()

s121 <- bind_rows(s121.total, s121.mnc, s121.local, s121.az) %>% 
  filter(Year >= '2018', 
         TA %in% c('RE', 'CV', 'DM', 'GI')) %>% 
  arrange(City_E, TA, Growth) %>% 
  pivot_wider(id_cols = c(City_E, TA, Growth), 
              names_from = YQ, 
              values_from = growth)


##---- S12-2 ----
s122.header <- read_excel('02_Inputs/S12-2.xlsx')
s122.order <- read_excel('02_Inputs/S12-S15 -2展示顺序.xlsx')

s122.az <- az.delivery.final %>% 
  group_by(Year, YQ, City_E, TA, Market, Prod_Ename, Corp_EName, Corp_TYPE) %>% 
  summarise(value = sum(`Value (RMB)` / 7.2, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Market, Prod_Ename) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(Year, City_E, TA, Market) %>% 
  mutate(`AZ MS%` = value / sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market, Prod_Ename) %>% 
  arrange(Year) %>% 
  mutate(`AZ ΔMS%` = `AZ MS%` - lag(`AZ MS%`), 
         `AZ GR%` = value / lag(value) - 1) %>% 
  ungroup()

s122.market <- az.delivery.final %>% 
  group_by(Year, YQ, City_E, TA, Market, Prod_Ename, Corp_EName, Corp_TYPE) %>% 
  summarise(value = sum(`Value (RMB)` / 7.2, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(stri_sub(YQ, 5, 6) %in% ytd.quarter) %>% 
  group_by(Year, City_E, TA, Market) %>% 
  summarise(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  group_by(City_E, TA, Market) %>% 
  arrange(Year) %>% 
  mutate(`MKT GR%` = value / lag(value) - 1) %>% 
  ungroup() %>% 
  select(Year, City_E, TA, Market, `MKT GR%`)

s122.max <- s122.az %>% 
  filter(!is.na(`AZ MS%`)) %>% 
  group_by(Year, City_E, TA, Market) %>% 
  filter(`AZ MS%` == max(`AZ MS%`, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(Year, City_E, TA, Market, `Max MS Player` = Prod_Ename, 
         `Max MS Player MS` = `AZ MS%`)

s122.delta <- s122.az %>% 
  filter(!is.na(`AZ ΔMS%`)) %>% 
  group_by(Year, City_E, TA, Market) %>% 
  filter(`AZ ΔMS%` == max(`AZ ΔMS%`, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(Year, City_E, TA, Market, `Max ΔMS Player` = Prod_Ename, 
         `Max ΔMS Player ΔMS` = `AZ ΔMS%`)

s122 <- s122.header %>% 
  left_join(s122.az, by = c('TA', 'Market', 'Prod_Ename')) %>% 
  left_join(s122.market, by = c('Year', 'City_E', 'TA', 'Market')) %>% 
  left_join(s122.max, by = c('Year', 'City_E', 'TA', 'Market')) %>% 
  left_join(s122.delta, by = c('Year', 'City_E', 'TA', 'Market')) %>% 
  filter(Year == max(table.year)) %>% 
  arrange(City_E, TA, Market, Prod_Ename) %>% 
  right_join(s122.order, by = c('TA', 'Display Name')) %>% 
  select(City_E, TA, Market, Prod_Ename, `Display Name`, `AZ MS%`, `AZ ΔMS%`, 
         `AZ GR%`, `MKT GR%`, `Max MS Player`, `Max MS Player MS`, 
         `Max ΔMS Player`, `Max ΔMS Player ΔMS`)


##---- Write out ----
wb <- createWorkbook()
addWorksheet(wb, 'S3-1')
addWorksheet(wb, 'S3-2')
addWorksheet(wb, 'S3-3')
addWorksheet(wb, 'S4-1')
addWorksheet(wb, 'S4-2')
addWorksheet(wb, 'S5-8')
addWorksheet(wb, 'S10-1')
addWorksheet(wb, 'S10-2')
addWorksheet(wb, 'S11-1')
addWorksheet(wb, 'S11-2')
addWorksheet(wb, 'S12-1')
addWorksheet(wb, 'S12-2')

writeDataTable(wb, 'S3-1', s31)
writeDataTable(wb, 'S3-2', s32)
writeDataTable(wb, 'S3-3', s33)
writeDataTable(wb, 'S4-1', s41)
writeDataTable(wb, 'S4-2', s42)
writeDataTable(wb, 'S5-8', s58)
writeDataTable(wb, 'S10-1', s101)
writeDataTable(wb, 'S10-2', s102)
writeDataTable(wb, 'S11-1', s111)
writeDataTable(wb, 'S11-2', s112)
writeDataTable(wb, 'S12-1', s121)
writeDataTable(wb, 'S12-2', s122)

saveWorkbook(wb, '03_Outputs/AZ_CHC_Summary_Table.xlsx', overwrite = TRUE)





