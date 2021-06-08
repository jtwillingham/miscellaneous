input_dir <- ""

options(warn = -1)
my_packages <- c("dplyr", "magrittr", "stringr")
lapply(my_packages, install.packages, repos = "https://cran.rstudio.com/", quiet = TRUE)
suppressPackageStartupMessages(lapply(my_packages, library, character.only = TRUE, quietly = TRUE))

func_start_row <-
  function(string, input_df) {
    which(grepl(string, input_df$connection)) + 1L
  }

followers <- paste(input_dir, "followers.txt", sep = "")
load_followers <- tibble(connection = str_squish(scan(followers, what = character(), sep = "\n")))
followers_start <- func_start_row("People who most recently followed you", load_followers)
followers_df <- load_followers[followers_start:nrow(load_followers), ] %>%
  dplyr::filter(!grepl("followers", connection)) %>%
  dplyr::filter(!grepl("Followed", connection)) %>%
  dplyr::filter(row_number() %% 3 == 1)

following <- paste(input_dir, "following.txt", sep = "")
load_following <- tibble(connection = str_squish(scan(following, what = character(), sep = "\n")))
following_start <- func_start_row("Filter by All", load_following)
following_df <- load_following[following_start:nrow(load_following), ] %>%
  dplyr::filter(!grepl("post this week", connection)) %>%
  dplyr::filter(!grepl("posts this week", connection)) %>%
  dplyr::filter(!grepl("followers", connection)) %>%
  dplyr::filter(row_number() %% 3 == 1)

unfollowed_df <- anti_join(following_df, followers_df)
