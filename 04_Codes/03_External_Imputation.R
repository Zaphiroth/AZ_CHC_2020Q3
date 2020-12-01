# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ProjectName:  AZ CHC 2020Q3
# Purpose:      External imputation
# programmer:   Zhe Liu
# Date:         2020-12-01
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


##---- KNN model ----
## model set
model.data <- imp.internal %>% 
  mutate(flag_sample = ifelse(province %in% c('广东'), 1, 0))

model.set <- model.data %>% 
  filter(year %in% c('2019')) %>% 
  group_by(province, city, district, pchc, product, flag_sample) %>% 
  summarise(sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  pivot_wider(id_cols = c(province, city, district, pchc, flag_sample), 
              names_from = product, 
              values_from = sales, 
              values_fill = 0)

## knn model
train.set <- model.set[model.set$flag_sample == 0, ]
test.set <- model.set[model.set$flag_sample == 1, ]

knn.model <- kknn(flag_sample ~ ., 
                  train = train.set[, -(1:4)], 
                  test = test.set[, -(1:4)], 
                  k = 3, 
                  scale = TRUE)

## model weightage
model.indice <- as.data.frame(knn.model$C) %>% 
  lapply(function(x) {
    train.set$pchc[x]
  }) %>% 
  as.data.frame(col.names = c('pchc_1', 'pchc_2', 'pchc_3')) %>% 
  bind_cols(test.set[, 1:4]) %>% 
  pivot_longer(cols = c(pchc_1, pchc_2, pchc_3), 
               names_to = 'knn_level', 
               values_to = 'knn_pchc')

model.weight <- as.data.frame(knn.model$D) %>% 
  lapply(function(x) {
    1 / (x+1)
  }) %>% 
  as.data.frame(col.names = c('pchc_1', 'pchc_2', 'pchc_3')) %>% 
  mutate(pchc_1 = pchc_1 / pchc_1 + pchc_2 + pchc_3,
         pchc_2 = pchc_2 / pchc_1 + pchc_2 + pchc_3,
         pchc_3 = pchc_3 / pchc_1 + pchc_2 + pchc_3) %>% 
  bind_cols(test.set[, 1:4]) %>% 
  pivot_longer(cols = c(pchc_1, pchc_2, pchc_3), 
               names_to = 'knn_level', 
               values_to = 'knn_weight')


##---- Imputing Inexistent provinces ----
## molecule growth
model.growth <- model.data %>% 
  filter(flag_sample == 0) %>% 
  group_by(knn_pchc = pchc, molecule, year) %>% 
  summarise(sales = sum(sales, na.rm = TRUE)) %>% 
  ungroup() %>% 
  inner_join(model.indice, by = 'knn_pchc') %>% 
  left_join(model.weight, by = c('province', 'city', 'district', 'pchc', 'knn_level')) %>% 
  group_by(pchc, molecule, year) %>% 
  summarise(sales = sum(sales * knn_weight, na.rm = TRUE)) %>% 
  ungroup() %>% 
  pivot_wider(id_cols = c(pchc, molecule), 
              names_from = year, 
              values_from = sales, 
              values_fill = 0) %>% 
  mutate(growth = `2020` / `2019`, 
         growth = if_else(is.na(growth) | growth < 0.1 | growth > 10, 1, growth)) %>% 
  select(pchc, molecule, growth)

## imputed
imp.external <- model.data %>% 
  filter(flag_sample == 1) %>% 
  left_join(model.growth, by = c('pchc', 'molecule')) %>% 
  mutate(growth = if_else(is.na(growth), 1, growth),
         growth = if_else(growth > 3, 3, growth),
         growth = if_else(growth > quantile(growth, 0.9),
                          mean(growth[growth >= quantile(growth, 0.25) & 
                                        growth <= quantile(growth, 0.75)]),
                          growth)) %>% 
  mutate(sales = sales / growth,
         date = gsub('2019', '2020', date),
         quarter = gsub('2019', '2020', quarter),
         year = '2020', 
         flag1 = 0, 
         flag2 = 1) %>% 
  select(year, date, quarter, province, city, district, pchc, TA, flag_mkt, 
         atc4, molecule, product, packid, sales, flag1, flag2)


##---- Imputation result ----
imp.total <- imp.internal %>% 
  mutate(flag2 = 0) %>% 
  bind_rows(imp.external)

write_feather(imp.total, '03_Outputs/03_AZ_CHC_External_Imp.feather')
