library(magrittr)

location <- "~/Codes/GCS-Make-Postcode-LAD-Lookup/inputs/PCD_OA_LSOA_MSOA_LAD_FEB22_UK_LU.csv"

lookup_raw <- 
  readr::read_csv(
    location,
    col_select = c("pcds", "ladnm")
  ) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(
    outcode = stringr::str_split(string = pcds, pattern = " ") %>% purrr::pluck(1, 1),
    first_letter = stringr::str_extract(outcode, pattern = "^[a-zA-Z]{1}")
  ) %>% 
  dplyr::ungroup()

max_rows_allowed_per_df = 1000000

num_rows_total <- nrow(lookup_raw)

num_dfs_required <- ceiling(num_rows_total / max_rows_allowed_per_df)
  
find_split_row <- function(df){
    
  df %>% 
    dplyr::select(first_letter) %>% 
    dplyr::mutate(row_num = dplyr::row_number()) %>% 
    dplyr::group_by(first_letter) %>% 
    dplyr::summarise(start_row = min(row_num)) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(split_index = start_row - max_rows_allowed_per_df) %>% 
    dplyr::filter(split_index < 0) %>% 
    dplyr::filter(split_index == max(split_index)) %>% 
    dplyr::pull(start_row)
}

split_ends <- c()

for (i in 1:num_dfs_required) {
  if(i ==1){
    
    x <- 
      lookup_raw %>% 
      find_split_row()
    
    split_ends <- append(split_ends, x)
    
  } else if (i > 1){
    
    previous_split <- split_ends[i-1]
    
    x <- 
      lookup_raw %>% 
      dplyr::filter(dplyr::row_number() > previous_split) %>% 
      find_split_row() + previous_split
    
    split_ends <- append(split_ends, x)
    
  }
} 

lad_code_lookup <- 
  unique(lookup_raw$ladnm) %>% 
  tibble::as_tibble() %>% 
  tidyr::drop_na() %>% 
  dplyr::mutate(
    row_num = dplyr::row_number()
  ) %>% 
  dplyr::mutate(
    gcs_data_audit_code = row_num + 99
  ) %>% 
  dplyr::select(
    ladnm = value,
    gcs_data_audit_code,
    -row_num
  )

sysdatetime <- Sys.time() %>% 
  format("%Y-%m-%d_%H-%M-%S_%Z")
  
lad_code_lookup %>% 
  dplyr::select(
    `LAD Name` = "ladnm",
    `LAD Code (GCS Data Audit)` = "gcs_data_audit_code"
  ) %>% 
  dplyr::arrange(`LAD Name`) %>% 
  readr::write_excel_csv(
    paste0("~/Codes/GCS-Make-Postcode-LAD-Lookup/outputs/", sysdatetime, " LAD Name to Data Audit Code lookup.csv")
  )

lookup <- 
  lookup_raw %>% 
  dplyr::left_join(
    lad_code_lookup
  ) %>%
  dplyr::select(
    Postcode = pcds,
    `Local Authority District (LAD)` = ladnm,
    `LAD Code (GCS Data Audit)` = gcs_data_audit_code
  )


readr::write_excel_csv(
  lookup,
  paste0("~/Codes/GCS-Make-Postcode-LAD-Lookup/outputs/", sysdatetime, " Postcode to LAD lookup OFFICIAL.csv")
)

other_values <- 
  tibble::tibble(
    Postcode = c("International", "Other postcode", "Unknown"),
    `Local Authority District (LAD)` = c("International - not applicable", "Other LAD", "Unknown"),
    `LAD Code (GCS Data Audit)` = c(98, 99, "**")
  )

readr::write_excel_csv(
  other_values,
  paste0("~/Codes/GCS-Make-Postcode-LAD-Lookup/outputs/", sysdatetime, " Other values LAD lookup OFFICIAL.csv")
)


lookup_split <- 
  lookup %>% 
  ## We have to add one, because the split_ends records the start of the next
  ## split rather than the end of the previous.
  dplyr::mutate(row_to_match = dplyr::row_number() + 1) %>% 
  # dplyr::filter(dplyr::between(row_to_match, split_ends[2] - 5, split_ends[2] + 5)) %>% 
  dplyr::rowwise() %>% 
    dplyr::mutate(file_group = 
                    dplyr::case_when(
                      row_to_match %in% split_ends ~ match(row_to_match, split_ends),
                      T ~ NA_integer_
                    )
    ) %>% 
  dplyr::ungroup() %>% 
  tidyr::fill(file_group, .direction = "up") %>% 
  dplyr::group_split(file_group)


lookup_split %>% 
purrr::map(
  .f = function(df){
    
    first_letter_start <- 
      df %>% 
      dplyr::slice_head(n = 1) %>% 
      dplyr::pull(Postcode) %>% 
      stringr::str_extract(pattern = "^[a-zA-Z]{1}") %>% 
      purrr::pluck(1, 1)
    
    first_letter_end <-
      df %>% 
      dplyr::slice_tail(n = 1) %>% 
      dplyr::pull(Postcode) %>% 
      stringr::str_extract(pattern = "^[a-zA-Z]{1}") %>% 
      purrr::pluck(1, 1)
    
    fname <- paste0("~/Codes/GCS-Make-Postcode-LAD-Lookup/outputs/", sysdatetime, " ", first_letter_start, " - ", first_letter_end, " Postcode to LAD lookup OFFICIAL.csv")
    
    df %>% 
      dplyr::select(-c(row_to_match, file_group)) %>% 
      readr::write_excel_csv(file = fname)
  }
)


